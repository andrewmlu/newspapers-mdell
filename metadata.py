import os
import re
import json
import time
from datetime import datetime

FILE_PATH = "/mnt/data01/pipeline_backup/pipeline_egress/"


class ManifestCollection:
    """
    Class representing a collection of multiple manifests
    """
    def __init__(self, manifest_names, filename):
        """
        Initialize a ManifestCollection with a list of manifest names and the name of a file containing newspaper names
        """
        with open(filename, "r") as f:
            newspapers = f.read().splitlines()
        self.manifests = []
        start = time.time()
        for i, name in enumerate(manifest_names):
            print(time.time() - start)
            # if i > 46 and i < 53:
            print(name)
            self.manifests.append(Manifest(name, newspapers))
    @classmethod
    def from_directory(cls, dir_path, filename):
        """
        Class method to create a ManifestCollection object based on all manifests found in a directory
        """
        manifest_names = sorted([name for name in os.listdir(dir_path) if os.path.isdir(os.path.join(dir_path, name))])
        print(manifest_names)
        return cls(manifest_names, filename)
    def get_all_metadata(self):
        """
        Get all metadata from all manifests in the collection
        """
        all_metadata = {}
        for i, manifest in enumerate(self.manifests):
            for newspaper, metadata in manifest.metadata.items():
                if newspaper not in all_metadata:
                    all_metadata[newspaper] = metadata
                else:
                    all_metadata[newspaper].extend(metadata)
        return all_metadata
    def get_entry_counts_by_newspaper_and_year(self):
        """
        Get the number of entries for each newspaper for each year
        """
        entry_counts = {}
        for newspaper, entries in self.get_all_metadata().items():
            entry_counts[newspaper] = {}
            for entry in entries:
                year = entry['Date'].split('-')[0]
                if year not in entry_counts[newspaper]:
                    entry_counts[newspaper][year] = 1
                else:
                    entry_counts[newspaper][year] += 1
        return entry_counts
    def get_entry_counts_by_year(self):
        entry_counts = {}
        for newspaper, entries in self.get_all_metadata().items():
            for entry in entries:
                year = entry['Date'].split('-')[0]
                if year not in entry_counts:
                    entry_counts[year] = 1
                else:
                    entry_counts[year] += 1
        return entry_counts
    def export_to_json(self, filename):
        """
        Export metadata to a JSON file
        """
        with open(filename, 'w') as f:
            json.dump(self.get_all_metadata(), f, indent=4)


class Manifest:
    """
    Class representing a Manifest which contains multiple minibatches
    """
    def __init__(self, name, newspapers):
        """
        Initialize Manifest with its name and newspapers (set of newspaper names)
        """
        self.name = name
        self.newspapers = set(newspapers)
        self.metadata = self.load_metadata()
    @staticmethod
    def from_file(manifest_name, filename):
        """
        Static method to create a Manifest object from a file
        """
        with open(filename, "r") as f:
            newspapers = f.read().splitlines()
        return Manifest(manifest_name, newspapers)
    def load_metadata(self):
        """
        Load metadata from all minibatches in the manifest for all newspapers in the set
        """
        metadata = {newspaper: [] for newspaper in self.newspapers}
        subdirs = self.list_subdirs()
        for i, subdir in enumerate(subdirs):
            # print(subdir)
            # if i < 20:  # Limiting to the first 20 directories for now
            minibatch = Minibatch(self.name, subdir)
            metadata = minibatch.append_metadata(metadata)
        return metadata
    def list_subdirs(self):
        """
        List all subdirectories in the manifest directory
        """
        path = os.path.join(FILE_PATH, self.name)
        subdirs = [name for name in os.listdir(path) if os.path.isdir(os.path.join(path, name))]
        return subdirs

class Minibatch:
    """
    Class representing a Minibatch which is part of a Manifest
    """
    def __init__(self, manifest_name, name):
        """
        Initialize Minibatch with its manifest_name and its own name
        """
        self.manifest_name = manifest_name
        self.name = name
        self.data = self.load_data()
    def load_data(self):
        """
        Load minibatch data from the ocr_text.json file
        """
        try:
            path = os.path.join(FILE_PATH, self.manifest_name, self.name, "ocr_text.json")
            with open(path, "r") as f:
                data = json.load(f)
        except FileNotFoundError:
            data = {}
        return data
    def append_metadata(self, metadata):
        """
        Extract metadata from the filenames in the minibatch data and append it to the provided metadata dictionary
        """
        for key, _ in self.data.items():
            item_metadata = self.extract_metadata_from_filename(key)
            newspaper = item_metadata.get('Newspaper')
            if newspaper in metadata:
                # print(newspaper)
                metadata[newspaper].append(item_metadata)
        return metadata
    @staticmethod
    def extract_metadata_from_filename(filename):
        """
        Extract metadata from a filename
        """
        pattern = r"(\d+)-(.+?)-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-(\d+)-(\d+)-p-(\d+)\.jpg"
        match = re.match(pattern, filename)
        dict = {}
        if match:
            date_str = f"{match.group(5)}-{match.group(3)}-{match.group(4)}"
            date = datetime.strptime(date_str, "%Y-%b-%d")
            dict = {
                "ID": match.group(1),
                "Newspaper": match.group(2).replace("-", " "),
                "Date": date.strftime("%Y-%m-%d"),
                "Pg": match.group(6)
        }
        return dict

if __name__ == "__main__":
    collection = ManifestCollection.from_directory(FILE_PATH, "newspapers.txt")
    print(collection.get_entry_counts_by_newspaper_and_year())
    print(collection.get_entry_counts_by_year())
    collection.export_to_json("metadata.json")