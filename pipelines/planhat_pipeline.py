import dlt
from planhat import planhat_source


def load_planhat():
    """Constructs a pipeline that will load all planhat data"""
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="planhat", destination="bigquery", dataset_name="planhat_data"
    )
    load_info = pipeline.run(planhat_source())
    print(load_info)


def load_selected_data():
    """Shows how to load just selected tables using `with_resources`"""
    pipeline = dlt.pipeline(
        pipeline_name="planhat",
        destination="bigquery",
        dataset_name="planhat_data",
    )
    load_info = pipeline.run(
        planhat_source().with_resources(
            "churn",
            "companies",
            "conversations",
            "customfields",
            "endusers",
            "invoices",
            "licenses",
            "nps",
            "tasks",
            "users",
        )
    )
    print(load_info)
    # just to show how to access resources within source
    planhat_data = planhat_source()
    # print source info
    print(planhat_data)
    print()
    # list resource names
    print(planhat_data.resources.keys())
    print()
    # print `companies` resource info
    print(planhat_data.resources["companies"])
    print()


if __name__ == "__main__":
    # run our main example
    load_planhat()
    # load selected tables and display resource info
    load_selected_data()
