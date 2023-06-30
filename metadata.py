import os
import re
import json
from collections import defaultdict
from datetime import datetime

FILE_PATH = "/mnt/data01/pipeline_backup/pipeline_egress/"

class ManifestCollection:
    """
    Class representing a collection of multiple manifests
    """
    def __init__(self):
        self.manifests = {}
    @classmethod
    def from_directory(cls, dir_path, newspaper_filename):
        with open(newspaper_filename, "r") as f:
            newspapers = f.read().splitlines()
        manifest_names = sorted([name for name in os.listdir(dir_path) if os.path.isdir(os.path.join(dir_path, name))])
        collection = cls()
        for name in manifest_names:
            collection.manifests[name] = Manifest(name, newspapers)
            collection.manifests[name].load_metadata(dir_path)
        return collection
    @classmethod
    def from_json(cls, json_file, newspaper_filename):
        with open(newspaper_filename, "r") as f:
            newspapers = f.read().splitlines()
        with open(json_file, 'r') as f:
            data = json.load(f)
        collection = cls()
        for newspaper, entries in data.items():
            for entry in entries:
                year = entry['Date'].split('-')[0]
                if year not in collection.manifests:
                    collection.manifests[year] = Manifest(year, newspapers)
                collection.manifests[year].add_to_metadata(entry)
        return collection
    def get_all_metadata(self):
        """
        Get all metadata from all manifests in the collection
        """
        all_metadata = {}
        for manifest in self.manifests.values():
            for newspaper, metadata in manifest.metadata.items():
                if newspaper not in all_metadata:
                    all_metadata[newspaper] = []
                all_metadata[newspaper].extend(metadata)
        return all_metadata
    def get_entry_counts_by_ordered_filter(self, filter_order, filter_values=None):
        """
        Get the number of entries for each specified hierarchical filter
        """
        entry_counts = {}
        # If no filter_values are given, create an empty list of lists with the same length as filter_order
        if filter_values is None:
            filter_values = [[] for _ in filter_order]
        # If filter_values is given but not a list of lists, raise an error
        if any(not isinstance(item, list) for item in filter_values):
            raise ValueError("filter_values must be a list of lists")
        # If filter_values is given but of wrong length, raise an error
        if len(filter_values) != len(filter_order):
            raise ValueError("filter_values must be the same length as filter_order")
        all_metadata = self.get_all_metadata()
        for newspaper, entries in all_metadata.items():
            for entry in entries:
                current_dict = entry_counts
                for i, filter_key in enumerate(filter_order):
                    filter_value = self.extract_filter_value(entry, filter_key)
                    if len(filter_values) == 0 \
                            or len(filter_values[i]) == 0 \
                            or filter_value in filter_values[i]:  # only consider this value if it's in the acceptable values list
                        if filter_value not in current_dict:
                            if filter_key == filter_order[-1]:  # if it's the last filter key, initialize count to 0
                                current_dict[filter_value] = 0
                            else:  # otherwise, initialize new dict to go deeper
                                current_dict[filter_value] = {}
                        if filter_key == filter_order[-1]:  # if it's the last filter key, increment count
                            current_dict[filter_value] += 1
                        else:  # otherwise, go deeper into the hierarchy
                            current_dict = current_dict[filter_value]
        return ManifestCollection.deep_sort_dict(entry_counts)
    @staticmethod
    def extract_filter_value(entry, filter_key):
        """
        Extract the required filter value from the entry
        """
        if filter_key == 'year':
            return entry['Date'].split('-')[0]
        elif filter_key == 'month':
            return entry['Date'].split('-')[1]
        elif filter_key == 'pg':
            return entry['Pg']
        elif filter_key == 'newspaper':
            return entry['Newspaper']
        else:
            raise ValueError(f"Invalid filter_key: {filter_key}")
    @staticmethod
    def deep_sort_dict(d):
        """
        Sort a dictionary's keys at all levels
        """
        def try_int(s):
            try:
                return int(s)
            except ValueError:
                return s
        return {k: ManifestCollection.deep_sort_dict(v) if isinstance(v, dict) else v
                for k, v in sorted(d.items(), key=lambda item: try_int(item[0]))}
    @staticmethod
    def count_entries(nested_dict):
        if isinstance(nested_dict, int):  # base case: it's an int, just return it
            return nested_dict
        else:  # recursive case: it's a dict, recursively count entries for each value and add them up
            return sum(ManifestCollection.count_entries(value) for value in nested_dict.values())
    @staticmethod
    def plot_histogram(counts, labels):
        """
        Plots a histogram of the given counts
        :param counts: List of count dictionaries
        :return: None
        """
        import numpy as np
        import matplotlib.pyplot as plt
        if len(counts) != len(labels):
            raise ValueError("counts and labels must be the same length")
        # Get the superset of all years
        all_years = sorted(set().union(*[list(c.keys()) for c in counts]))
        bar_width = 0.8 / len(counts)  # Adjust the bar width based on the number of count dictionaries
        index = np.arange(len(all_years))
        plt.figure(figsize=(15, 7))
        for i, count in enumerate(counts):
            # Fill in zeros for years not in this count
            entry_counts = [count.get(year, 0) for year in all_years]
            plt.bar(index + i * bar_width, entry_counts, bar_width, label=labels[i])
        plt.xlabel('Year')
        plt.ylabel('Number of Entries')
        plt.title('Number of Entries by Year')
        plt.xticks(index + bar_width / 2, all_years, rotation=90)
        plt.legend()
        plt.tight_layout()
        plt.show()
    def export_to_json(self, filename):
        """
        Export metadata to a JSON file
        """
        with open(filename, 'w') as f:
            json.dump(self.get_all_metadata(), f, indent=4)
    # def get_entry_counts_by_newspaper_and_year(self):
    #     """
    #     Get the number of entries for each newspaper for each year
    #     Deprecated by get_entry_counts_by_ordered_filter(['newspaper', 'year'])
    #     """
    #     entry_counts = {}
    #     for newspaper, entries in self.get_all_metadata().items():
    #         if newspaper not in entry_counts:
    #             entry_counts[newspaper] = {}
    #         for entry in entries:
    #             year = entry['Date'].split('-')[0]
    #             if year not in entry_counts[newspaper]:
    #                 entry_counts[newspaper][year] = 0
    #             entry_counts[newspaper][year] += 1
    #     return entry_counts
    # def get_entry_counts_by_year(self):
    #     """
    #     Get the number of entries for each year
    #     Deprecated by get_entry_counts_by_ordered_filter(['newspaper', 'year'])
    #     """
    #     entry_counts = {}
    #     for year, manifest in self.manifests.items():
    #         if year not in entry_counts:
    #             entry_counts[year] = 0
    #         entry_counts[year] = sum(len(entries) for entries in manifest.metadata.values())
    #     return entry_counts

class Manifest:
    """
    Class representing a Manifest which contains multiple minibatches
    """
    def __init__(self, name, newspapers):
        self.name = name
        self.newspapers = set(newspapers)
        self.metadata = {newspaper: [] for newspaper in self.newspapers}
    def load_metadata(self, dir_path):
        subdirs = self.list_subdirs(dir_path)
        for subdir in subdirs:
            minibatch = Minibatch(self.name, subdir)
            minibatch_metadata = minibatch.load_metadata(self.newspapers)
            self.add_to_metadata(minibatch_metadata)
    def add_to_metadata(self, minibatch_metadata):
        newspaper, entry = minibatch_metadata['Newspaper'], minibatch_metadata
        self.metadata[newspaper].append(entry)
    @staticmethod
    def list_subdirs(dir_path):
        return [name for name in os.listdir(dir_path) if os.path.isdir(os.path.join(dir_path, name))]


class Minibatch:
    """
    Class representing a minibatch of a manifest
    """
    def __init__(self, manifest_name, subdir):
        """
        Initialize minibatch with its manifest's name and its own subdirectory name
        """
        self.manifest_name = manifest_name
        self.subdir = subdir
    def load_metadata(self, newspapers):
        """
        Load metadata for all newspapers in the set from the minibatch
        """
        metadata = {newspaper: [] for newspaper in newspapers}  # Pre-populate with all known newspapers
        for newspaper in newspapers:
            newspaper_path = os.path.join(FILE_PATH, self.manifest_name, self.subdir, newspaper, "metadata.json")
            if os.path.exists(newspaper_path):
                with open(newspaper_path, 'r') as f:
                    data = json.load(f)
                for entry in data:
                    metadata[newspaper].append(entry)
        return metadata


collection = ManifestCollection.from_directory(FILE_PATH, "/mnt/data01/aml-newspapers/newspapers-mdell/newspapers.txt")
print(collection.get_entry_counts_by_newspaper_and_year())
print(collection.get_entry_counts_by_year())
collection.export_to_json("metadata.json")

# Testing the import from JSON function
collection = ManifestCollection.from_json("/mnt/data01/aml-newspapers/newspapers-mdell/metadata.json", "/mnt/data01/aml-newspapers/newspapers-mdell/newspapers.txt")
print(collection.get_entry_counts_by_newspaper_and_year())
print(collection.get_entry_counts_by_year())

collection.get_entry_counts_by_ordered_filter(['pg'])
first_pages = collection.get_entry_counts_by_ordered_filter(['year','pg'], [[],['1']])
for year, count in first_pages.items():
    first_pages[year] = first_pages[year]['1']

collection.plot_histogram(first_pages)
total_pages = collection.get_entry_counts_by_ordered_filter(['year'])

year_pages = collection.get_entry_counts_by_ordered_filter(['year','newspaper'], [[],[]])
for year, entry in year_pages.items():
    year_pages[year] = len(entry.keys())

ManifestCollection.plot_histogram([first_pages, total_pages], ['Cover Pages', 'Total'])
ManifestCollection.plot_histogram([year_pages], ['Newspapers per Year'])

testsum0 = 0
for newspaper, entries in collection.get_all_metadata().items():
    testsum0 += len(entries)

testsum1 = 0
for newspaper, entries in collection.get_entry_counts_by_newspaper_and_year().items():
    for year, count in entries.items():
        testsum1 += count

testsum2 = 0
for year, count in collection.get_entry_counts_by_year().items():
    testsum2 += count

