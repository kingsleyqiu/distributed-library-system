# Team collaboration

## Join the repository

The owner invites teammates through **Settings > Collaborators > Add people**.
Accept the invitation before cloning this private repository.

```bash
git clone https://github.com/kingsleyqiu/distributed-library-system.git
cd distributed-library-system
git switch main
git pull --ff-only origin main
```

Kingsley's branch already exists:

```bash
git switch --track origin/kingsley
```

Other teammates create their own branch from the updated `main`. Replace
`your-name` with your name, for example `aryan`:

```bash
git switch -c your-name
git push -u origin your-name
```

## Develop on your branch

For Kingsley (other teammates substitute their branch name):

```bash
git switch kingsley
git fetch origin
git merge origin/main
```

Start with a clean working tree. Commit your current work before synchronizing.
If a merge reports conflicts, edit the marked files, stage the resolved files,
and commit the merge. Use `git merge --abort` if you need to cancel the merge.
Do not discard another teammate's changes just to resolve a conflict.

After editing, run the relevant checks and inspect what you are committing:

```bash
git status
git diff
git add path/to/changed-file
git diff --cached
git commit -m "Describe the change"
git push
```

Replace `path/to/changed-file` with the files you intend to include. Never commit
passwords, tokens, `.env` files, or local runtime databases.

## Merge through a Pull Request

1. Open **Pull requests > New pull request** on GitHub.
2. Set **base: main** and **compare: your personal branch**.
3. Describe what changed and how you checked it; request another teammate's review.
4. Resolve conflicts and review conversations, then obtain at least one approval.
5. Merge using **Create a merge commit**. Keep your personal branch for more work.
6. Before the next change, run `git fetch origin` and `git merge origin/main`
   while on your personal branch, then `git push` to synchronize it.

Do not push directly to `main` or force-push shared history. A separate branch
isolates unfinished commits, but review and testing are still needed to keep
merged code working. Only update a local `main` with `git pull --ff-only`.

## Main branch protection

The saved protection configuration for `main` is:

- Require a PR and one approving review; dismiss stale approvals after new commits.
- Require review conversations to be resolved before merging.
- Apply the rules to administrators too (no bypass).
- Block force pushes and branch deletion.

**Verified during setup:** GitHub reports that this rule is **not enforced**
on this private repository under the current plan. Direct pushes to `main`
are therefore not blocked. These rules currently remain team conventions.

[GitHub's documentation](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches)
lists GitHub Pro as supporting protection for private personal repositories.
Eligible students can obtain Pro through GitHub Education. After activating a
supported plan, verify enforcement in **Settings > Branches** before relying
on the rule.

The PR author cannot supply their own required approval. Invite at least one
teammate before relying on a mandatory one-review rule. Add required status
checks once the project has real automated tests; none are configured yet.

Keep automatic branch deletion off and retain the merge-commit option for the
long-running personal branches.
