import json

FILE_PATH = "/mnt/data01/pipeline_backup/pipeline_egress/"

def load_data_from_minibatch(minibatch_path):
    # read from .ocr_text.json
    with open(minibatch_path + "ocr_text.json", "r") as f:
        metadata = json.load(f)
    return metadata

def list_all_subdirs(dir_path):
    import os
    subdirs = [name for name in os.listdir(dir_path) if os.path.isdir(os.path.join(dir_path, name))]
    return subdirs

def load_metadata_from_manifest(manifest_name, newspaper_set):
    # read each mini_batch from each directory labeled mini_batch_xxx and pass into load_metadata_from_minibatch
    # list all directories in manifest_path
    subdirs = list_all_subdirs(FILE_PATH + manifest_name)
    # load metadata from each mini_batch
    metadata_by_newspaper = initialize_metadata_by_newspaper(newspaper_set)
    for i, subdir in enumerate(subdirs):
        if (i < 20):
            # print(subdir)
            minibatch_data = load_data_from_minibatch(FILE_PATH + manifest_name + "/" + subdir + "/")
            minibatch_keys = get_keys_from_minibatch(minibatch_data)
            minibatch_metadata = get_metadata_from_keys(minibatch_keys)
            selected_mini_batch_metadata = select_metadata_by_newspaper(minibatch_metadata, newspaper_set)
            append_metadata_by_newspaper(metadata_by_newspaper, selected_mini_batch_metadata)
    return metadata_by_newspaper

def load_newspaper_set(filename):
    # load newspaper list as set
    with open(filename, "r") as f:
        newspaper_list = f.read().splitlines()
    return set(newspaper_list)

def select_metadata_by_newspaper(metadata, newspaper_set):
    selected_metadata = {}
    for d in metadata:
        if d['Newspaper'] in newspaper_set:
            if d['Newspaper'] not in selected_metadata:
                selected_metadata[d['Newspaper']] = [d]
            else:
                selected_metadata[d['Newspaper']].append(d)
    return selected_metadata

def get_keys_from_minibatch(minibatch):
    # get the filenames from metadata
    keys = list(minibatch.keys())
    return keys

def extract_metadata_from_filename(filename):
    import re
    from datetime import datetime
    # Regular expression pattern
    pattern = r"(\d+)-(.+?)-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-(\d+)-(\d+)-p-(\d+)\.jpg"
    # Match the pattern
    match = re.match(pattern, filename)
    dict = {}
    if match:
        # Parse date
        date_str = f"{match.group(5)}-{match.group(3)}-{match.group(4)}"
        date = datetime.strptime(date_str, "%Y-%b-%d")
        # Create a dictionary
        dict = {
            "ID": match.group(1),
            "Newspaper": match.group(2).replace("-", " "),
            "Date": date.strftime("%Y-%m-%d"),  # formatted as YYYY-MM-DD
            "Pg": match.group(6)
        }
    return dict

def get_metadata_from_keys(keys):
    metadata = [extract_metadata_from_filename(key) for key in keys]
    return metadata

def initialize_metadata_by_newspaper(newspaper_set):
    metadata = {}
    for newspaper in newspaper_set:
        metadata[newspaper] = []
    return metadata

def append_metadata_by_newspaper(metadata_by_newspaper, metadata):
    for key, value in metadata.items():
        if key in metadata_by_newspaper:
            metadata_by_newspaper[key].extend(value)

def get_metadata_metadata(metadata_by_newspaper):
    metadata = {}
    for key, value in metadata_by_newspaper.items():
        metadata[key] = len(value)
        # metadata[key] = len([d for d in value if d['Pg'] == '1'])
    return metadata

# test_keys = get_keys_from_minibatch(test_metadata[0])
# print(len(test_keys))
# extract_metadata_from_filename(test_keys[0])
test_newsp_set = set(["cambridge daily jeffersonian"])
test_metadata = load_metadata_from_manifest("all1973_size50_julyManifest", test_newsp_set)
test_newsp_metadata = select_metadata_by_newspaper(test_metadata, test_newsp_set)
