import shutil
import argparse

SOURCE_SOURCE = "chess"

SOURCE_FOLDER = "sources/{}"
SOURCE_EXAMPLE_FILE = "sources/demo_{}_pipeline.py"
SOURCE_TEST_FOLDER = "tests/{}"

parser = argparse.ArgumentParser("new_source.py")
parser.add_argument(
    "source_name", help="An integer will be increased by 1 and printed.", type=str
)
args = parser.parse_args()

if __name__ == "__main__":
    source_name = args.source_name
    print(f"Creating new source {source_name}")
    print("Copying source files...")
    shutil.copytree(
        SOURCE_FOLDER.format(SOURCE_SOURCE), SOURCE_FOLDER.format(source_name)
    )
    shutil.copy2(
        SOURCE_EXAMPLE_FILE.format(SOURCE_SOURCE),
        SOURCE_EXAMPLE_FILE.format(source_name),
    )
    shutil.copytree(
        SOURCE_TEST_FOLDER.format(SOURCE_SOURCE),
        SOURCE_TEST_FOLDER.format(source_name),
    )

    print("Done")
