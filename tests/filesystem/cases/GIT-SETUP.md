# Git repo for testing

The `./git`folder contains a bare repo used for running tests for the `filesystem` dlt Source. 

# Usage

For example, use it to test a pipeline that reads files using the `gitpythonfs` fsspec implementation.

The repo is not needed for regular use of dlt.

For the tests to pass, use the tag (aka `ref`) called `unmodified-samples`.  Using HEAD (the default) is intended to fail tests due to modifications such as a file not having the expected file name.  It allows testing of the `ref` functionality of git-based fsspec implementations.

Some features of the repo are intentionally different to the containing repo (eg verified-sources repo) to help prevent mistakenly testing against (or modifying!) the wrong repo:

- The default branch is `cases-master`
- the sample files root folder is `samples`, not `tests`.

# Configuration

When to configure (build?):
- When setting up an environment - CI, local dev etc. (Unless it's now committed in verified-sources repo)
- After modifying any content in `../samples folder`

Ideally the repo will be created idempotently by a pytest fixture, `make` script or similar.  Until then, these are the manual steps to idempotently create/recreate:

1. Set working directory to `tests/filesystem/cases/git`
2. Check the current folder contains only `.git`. eg `ls -a`. It's also ok if
the current folder is empty.
3. Delete `.git` and all subfolders. ie, delete the repo.  `rm -rf`
4. Make a fresh repo using:

```
git init
git checkout -b cases-master
```

5. Copy in the folder `../../samples`. ie samples folder and all its contents. eg `cp -r ../../samples .`
6. Put some object in the repo:

```
git add --all
git commit -m "add standard sample files for tests"
git tag -a unmodified-samples -m "The sample test files with no modifications"
git mv samples/sample.txt samples/sample_renamed.txt
git commit -m "rename samples.txt to make tests fail"
```

5. Delete all working files, except `.git`. eg with `rm -rf samples`. (ToDo: that's not officially not a bare repo. Use `git clone --bare path/to/repo` instead. Maybe we create the repo in a temp folder and then bare clone it into `cases/git` folder, discard the temp folder.)

# Developing

Note that at least one IDE - VSCode - does not recognise this repo in the Explorer and Source Control tabs. Likely because it is a repo inside another repo.

If you are considering committing the repo to its containing repo - eg the verified-sources repo -  consider the effects on the size of the containing repo:

`du -sh ./.git/*`

That's about the same as the samples folder itself. BUT consider that the largest file, `./.git/objects` might change often if the repo is regenerated. So it might be better to ensure the repo is gitignored (so meta!) and "build" it in each environment where needed.

