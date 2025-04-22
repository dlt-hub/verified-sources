The `/ai` directory includes for popular LLM-enabled IDEs (Cursor, Continue, Cline, Claude, etc.).
Add them to your project using the `dlt ai setup $IDE` command ([reference](https://dlthub.com/docs/reference/command-line-interface#dlt-ai-setup)).
The way it works is very simple:

1. Each folder here corresponds to a particular IDE.
2. `setup` subcommand looks for a folder named `$IDE` and copies all files from it to current `dlt` project.
3. `.message` file is displayed, not copied.

We are pretty sure that command line interface will soon change to support many set of rules per editor focused on specific tasks.

If you want to share useful rules, follow the instructions in [CONTRIBUTING.md](../CONTRIBUTING.md#walkthrough-modify-or-add-rules-files-for-llm-enabled-ides).