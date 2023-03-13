import dlt

from hubspot import hubspot


def main():
    p = dlt.pipeline(pipeline_name='hubspot',
                     dataset_name='hubspot',
                     destination='postgres',
                     full_refresh=False)

    info = p.run(
        hubspot()
    )
    print(info)


if __name__ == "__main__":
    main()
