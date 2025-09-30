#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact.
"""
import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

# DO NOT MODIFY
def go(args):
    """
    Basic cleaning:
    - load CSV artifact from W&B
    - drop price outliers based on [min_price, max_price]
    - parse 'last_review' to datetime
    - (NYC bounds filter can be applied later per rubric)
    - log cleaned CSV back to W&B as a new artifact
    """
    # Single init (avoid double init)
    run = wandb.init(project="nyc_airbnb", job_type="basic_cleaning", group="cleaning", save_code=True)
    run.config.update(vars(args))

    # Download input artifact (this also records that we used it)
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact_local_path)

    # Drop outliers
    min_price = args.min_price
    max_price = args.max_price
    idx = df["price"].between(min_price, max_price)
    df = df[idx].copy()

    # Convert last_review to datetime (coerce problematic strings to NaT)
    df["last_review"] = pd.to_datetime(df["last_review"], errors="coerce")

    # 🔕 IMPORTANT: per rubric, add NYC bounds later for the new release.
    # If you do it now, your first release might not fail on sample2.csv.
    # idx = df["longitude"].between(-74.25, -73.50) & df["latitude"].between(40.5, 41.2)
    # df = df[idx].copy()

    # Save the cleaned file
    df.to_csv("clean_sample.csv", index=False)

    # Log the new data.
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)

    run.finish()


# TODO: In the code below, fill in the data type for each argumemt. The data type should be str, float or int.
# TODO: In the code below, fill in a description for each argument. The description should be a string.
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")

    parser.add_argument(
        "--input_artifact",
        type=str,
        help="Fully qualified input artifact to clean, e.g. 'sample.csv:latest'",
        required=True,
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="Name for the cleaned CSV artifact to create, e.g. 'clean_sample.csv'",
        required=True,
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help="Artifact type for the cleaned data, e.g. 'clean_sample'",
        required=True,
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help="Short description of the cleaned artifact contents",
        required=True,
    )

    parser.add_argument(
        "--min_price",
        type=float,
        help="Minimum nightly price to keep (inclusive)",
        required=True,
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help="Maximum nightly price to keep (inclusive)",
        required=True,
    )

    args = parser.parse_args()
    go(args)
