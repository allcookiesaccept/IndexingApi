from manager import IndexingManager


def main(urls, json_keys_folder):

    manager = IndexingManager(json_keys_folder)
    manager.index_urls(urls)


if __name__ == "__main__":

    with open("urls.csv", "r") as file:
        urls = file.read().splitlines()

    json_keys_folder = "json_keys"

    main(urls[:3], json_keys_folder)