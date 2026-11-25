"""Deterministic circulation and staff catalog rules shared by both storage backends."""

from datetime import timedelta

from library_system.consensus.state_machine import (
    CatalogStateMachine, CommandOutcome, CommandRejected, EntryContext,
)
from library_system.models import Book, BookCopy, CatalogSnapshot, CommandEnvelope, Hold


LOAN_DAYS = 14
MAX_RENEWALS = 2


def _changed(snapshot: CatalogSnapshot, book: Book, result: dict) -> CommandOutcome:
    return CommandOutcome(
        books=tuple(book if item.book_id == book.book_id else item for item in snapshot.books),
        result=result,
    )


def circulate(
    snapshot: CatalogSnapshot, command: CommandEnvelope, context: EntryContext,
) -> CommandOutcome:
    book = next((book for book in snapshot.books if book.book_id == command.book_id), None)

    if command.operation == "add_book":
        if book is not None:
            raise CommandRejected("book_already_exists")
        if not command.title or not command.author:
            raise CommandRejected("missing_book_metadata")
        branch = command.home_branch or "branch-1"
        count = command.num_copies or 1
        copies = tuple(
            BookCopy(
                copy_id=f"{command.book_id}-c{idx}",
                branch_id=branch,
                available=True,
                version=0,
            )
            for idx in range(1, count + 1)
        )
        new_book = Book(
            book_id=command.book_id,
            title=command.title.strip(),
            author=command.author.strip(),
            copies=copies,
        )
        return CommandOutcome(
            books=(*snapshot.books, new_book),
            result={"operation": "add_book", "book": new_book.model_dump(mode="json")},
        )

    if book is None:
        raise CommandRejected("book_not_found")

    if command.operation == "remove_book":
        if any(not item.available for item in book.copies):
            raise CommandRejected("copies_checked_out")
        remaining = tuple(item for item in snapshot.books if item.book_id != book.book_id)
        return CommandOutcome(
            books=remaining,
            result={"operation": "remove_book", "book_id": book.book_id},
        )

    if command.operation == "hold":
        if not book.copies:
            raise CommandRejected("no_copies")
        if any(copy.borrower_id == command.actor_id for copy in book.copies):
            raise CommandRejected("already_borrowed")
        if any(hold.actor_id == command.actor_id for hold in book.holds):
            raise CommandRejected("already_on_hold")
        hold = Hold(hold_id=command.request_id, actor_id=command.actor_id, created_at=context.timestamp)
        updated = book.model_copy(update={"holds": (*book.holds, hold)})
        return _changed(snapshot, updated, {
            "operation": "hold", "book_id": book.book_id,
            "hold": hold.model_dump(mode="json"), "position": len(updated.holds),
        })

    copy = next((copy for copy in book.copies if copy.copy_id == command.copy_id), None)
    if copy is None:
        raise CommandRejected("copy_not_found")
    if copy.version != command.expected_version:
        raise CommandRejected("version_conflict")

    holds = book.holds
    if command.operation == "borrow":
        if not copy.available:
            raise CommandRejected("copy_unavailable")
        if any(item.borrower_id == command.actor_id for item in book.copies):
            raise CommandRejected("already_borrowed")
        if holds and holds[0].actor_id != command.actor_id:
            raise CommandRejected("hold_queue_priority")
        if holds:
            holds = holds[1:]
        updated_copy = copy.model_copy(update={
            "available": False, "borrower_id": command.actor_id, "renewals": 0,
            "due_date": context.timestamp.date() + timedelta(days=LOAN_DAYS),
            "version": copy.version + 1,
        })
    elif command.operation in {"return", "renew"}:
        if copy.available:
            raise CommandRejected("copy_not_borrowed")
        if copy.borrower_id != command.actor_id:
            raise CommandRejected("not_borrower")
        if command.operation == "return":
            updated_copy = copy.model_copy(update={
                "available": True, "borrower_id": None, "due_date": None,
                "renewals": 0, "version": copy.version + 1,
            })
        else:
            if holds:
                raise CommandRejected("holds_pending")
            if copy.renewals >= MAX_RENEWALS:
                raise CommandRejected("renewal_limit")
            if copy.due_date < context.timestamp.date():
                raise CommandRejected("loan_overdue")
            updated_copy = copy.model_copy(update={
                "due_date": copy.due_date + timedelta(days=LOAN_DAYS),
                "renewals": copy.renewals + 1, "version": copy.version + 1,
            })
    else:
        raise CommandRejected("unsupported_operation")

    updated = book.model_copy(update={
        "copies": tuple(updated_copy if item.copy_id == copy.copy_id else item for item in book.copies),
        "holds": holds,
    })
    return _changed(snapshot, updated, {
        "operation": command.operation, "book_id": book.book_id,
        "copy": updated_copy.model_dump(mode="json"),
    })


def create_circulation_machine() -> CatalogStateMachine:
    return CatalogStateMachine(
        {operation: circulate for operation in ("borrow", "return", "renew", "hold", "add_book", "remove_book")}
    )