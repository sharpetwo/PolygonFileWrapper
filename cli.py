import argparse

from polygon_wrapper import PolygonFileWrapper


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--endpoint",
        choices=["day", "minutes", "trades", "quotes"],
        help='Type of data to download.',
        required=True
    )
    parser.add_argument(
        "--market",
        choices=["options", "stocks"],
        help='Asset class to download',
        required=True
    )
    parser.add_argument(
        "--start_date", help="Start date (inclusive), YYYYMMDD format.", required=True
    )
    parser.add_argument(
        "--end_date", help="End date (inclusive), YYYYMMDD format.", required=False
    )
    parser.add_argument(
        '--output_dir',
        type=str,
        help='The directory to save output files.',
        required=True
    )
    args = parser.parse_args()

    wrapper = PolygonFileWrapper(
        polygon_market=args.market,
        polygon_endpoint=args.endpoint,
        datadir=args.output_dir
    )
    wrapper.download_history_on_disk(args.start_date, args.end_date, clean=True)
