# Python imports
import io
import json
import sys
import time

# Google APIs imports
from apiclient.discovery import build
from apiclient.errors import HttpError
from apiclient.http import MediaIoBaseUpload

# Tutorial imports
import auth

## Runs through each BigQuery API request
def run_tour(service, project_id):
    print("Running BigQuery API tour")

    projects = service.projects()
    datasets = service.datasets()
    tables = service.tables()
    tabledata = service.tabledata()
    jobs = service.jobs()

    # Generate some IDs to use with the tour.
    tour = "tour_%d" % (time.time())
    dataset_id = "dataset_" + tour
    table_id = "table_" + tour
    job_id = "job_" + tour

    project_ref = {"projectId" : project_id}
    dataset_ref = {"datasetId" : dataset_id,
                   "projectId" : project_id}
    table_ref = {"tableId" : table_id,
                 "datasetId" : dataset_id,
                 "projectId" : project_id}
    job_ref = {"jobId" : job_id,
               "projectId" : project_id}

    # First find the project and print the friendly name
    for project in projects.list().execute()["projects"]:
        if(project["id"] == project_id):
            print("Found %s: %s" % (project_id, project["friendlyName"]))

    # Now create a dataset
    dataset = {"datasetReference" : dataset_ref}
    dataset = datasets.insert(body=dataset, **project_ref).execute()

    # Patch the dataset to a friendly name
    update = {"friendlyName" : "Tour dataset"}
    dataset = datasets.patch(body=update, **dataset_ref).execute()

    # Print out the dataset posterity
    print("%s" % (dataset,))

    # Find our dataset in the datasets list
    dataset_list = datasets.list(**project_ref).execute()
    for current in dataset_list["datasets"]:
        if current["id"] == dataset["id"]:
            print("Found %s" % dataset["id"])

    # Now onto tables
    table = {"tableReference" : table_ref}
    table = tables.insert(body=table, **dataset_ref).execute()

    # Update the table to add schema
    table["schema"] = {"fields" : [{"name" : "a", "type" : "string"}]}
    table = tables.update(body=update, **table_ref).execute()

    # Patch the table to add a friendly name
    patch = {"friendlyName" : "Friendly Table"}
    table = tables.patch(body=patch, **table_ref).execute()

    # Print table for posterity
    print(table)

    # Find out table in the tables list
    table_list = tables.list(**dataset_ref).execute()
    for current in table_list["tables"]:
        if current["id"] == table["id"]:
            print("Found %s" % (table["id"]))

    # And now for some jobs
    config = {"load" : {"destinationTable" : table_ref}}
    load_text = "first\nsecond\nthird"

    # Remember to always name your jobs
    job = {"jobReference" : job_ref, "configuration" : config}
    media = MediaIoBaseUpload(io.BytesIO(load_text.encode("utf-8")),
                              mimetype = "application/octet-stream")
    job = jobs.insert(body=job,
                      media_body=media,
                      **project_ref).execute()

    # List our running/pending job
    job_list = jobs.list(stateFilter=["pending", "running"],
                         **project_ref).execute()
    print(job_list)

    while job["status"]["state"] != "DONE":
        job = jobs.get(**job_ref).execute()

    # Now run a query against that table
    query = "select count(*) from [%s]" % {table["id"]}
    query_request = {"query" : query, "timeoutMs" : 0, "maxResults" : 1}
    results = jobs.query(body=query_request, **project_ref).execute()
    while not results["jobComplete"]:
        get_results_request = results["jobReference"].copy()
        get_results_request["timeoutMs"] = 10000
        get_results_request["maxResults"] = 10
        results = jobs.getQueryResults(
            **get_results_request).execute()
    print(results)

    # Now let's read the data from our table
    data = tabledata.list(**table_ref).execute()
    table = tables.get(**table_ref).execute()
    print("Table %s\nData:%s" % (data, table))

    # Now we should cleanup our toys
    tables.delete(**table_ref).execute()
    datasets.delete(**dataset_ref).execute()

    # Now try reading the dataset after deleting it
    try:
        datasets.get(**dataset_ref).execute()
        print("That's funny. We should never get here!")
    except HttpError as err:
        print("Expected error:\n%s" % (err,))

    # Done

def main(argv):
    service = auth.build_bq_client()
    project_id = "studied-sled-134801" if len(argv) == 0 else argv[0]
    run_tour(service, project_id)

if __name__ == "__main__":
    main(sys.argv[1:])