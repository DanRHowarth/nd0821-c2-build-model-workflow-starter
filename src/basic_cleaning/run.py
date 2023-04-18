#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    logger.info(f"Fetching artifact {args.input_artifact}")
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact_local_path)

    logger.info(f"Cleaning dataset based on max_price of {args.max_price} and min_price of {args.min_price}")
    # Drop outliers
    min_price = args.min_price
    max_price = args.max_price
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()

    # Convert last_review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])

    logger.info(f"Saving cleaned data as {args.output_artifact} to W&B")
    df.to_csv(args.output_artifact, index=False)


    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file(args.output_artifact)
    run.log_artifact(artifact)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help='name and version of input artifact to be cleaned in format of name:version',
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help='Specify the name of the artifact created as a result of this step and uploaded to W&B',
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help='Specify the type of artifact being created',
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help='Provide a description of the artifact',
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help='Set a minimum price to filter the data by',
        required=True
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help='Set a maximum price to filter the data by',
        required=True
    )

    args = parser.parse_args()

    go(args)
