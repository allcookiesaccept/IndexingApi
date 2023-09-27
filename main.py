from manager import IndexingManager


def main(urls_csv_file, json_keys_folder):
    with open(urls_csv_file, "r") as file:
        urls = file.read().splitlines()

    manager = IndexingManager(urls, json_keys_folder)
    manager.index_urls()


if __name__ == "__main__":
    urls_csv_file = "urls.csv"
    json_keys_folder = "json_keys"

    main(urls_csv_file, json_keys_folder)