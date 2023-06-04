import shutil
import argparse

SOURCE_PIPELINE = "chess"

PIPELINE_FOLDER = "pipelines/{}"
PIPELINE_EXAMPLE_FILE = "pipelines/{}_pipeline.py"
PIPELINE_TEST_FOLDER = "tests/{}"

parser = argparse.ArgumentParser("new_pipeline.py")
parser.add_argument(
    "pipeline_name", help="An integer will be increased by 1 and printed.", type=str
)
args = parser.parse_args()

if __name__ == "__main__":
    pipeline_name = args.pipeline_name
    print(f"Creating new pipeline {pipeline_name}")
    print("Copying pipeline files...")
    shutil.copytree(
        PIPELINE_FOLDER.format(SOURCE_PIPELINE), PIPELINE_FOLDER.format(pipeline_name)
    )
    shutil.copy2(
        PIPELINE_EXAMPLE_FILE.format(SOURCE_PIPELINE),
        PIPELINE_EXAMPLE_FILE.format(pipeline_name),
    )
    shutil.copytree(
        PIPELINE_TEST_FOLDER.format(SOURCE_PIPELINE),
        PIPELINE_TEST_FOLDER.format(pipeline_name),
    )

    print("Done")
