import json

FILE_PATH = "/mnt/data01/pipeline_backup/pipeline_egress/"

def load_metadata_from_minibatch(minibatch_path):
    # read from .ocr_text.json
    with open(minibatch_path + "ocr_text.json", "r") as f:
        metadata = json.load(f)
    return metadata

def list_all_subdirs(dir_path):
    import os
    subdirs = [name for name in os.listdir(dir_path) if os.path.isdir(os.path.join(dir_path, name))]
    return subdirs

def load_metadata_from_manifest(manifest_name):
    # read each mini_batch from each directory labeled mini_batch_xxx and pass into load_metadata_from_minibatch
    # list all directories in manifest_path
    subdirs = list_all_subdirs(FILE_PATH + manifest_name)
    # load metadata from each mini_batch
    metadata = []
    for i, subdir in enumerate(subdirs):
        # if (i < 20):
            # print(subdir)
        metadata.append(load_metadata_from_minibatch(FILE_PATH + manifest_name + "/" + subdir + "/"))
    return metadata

def load_newspaper_list(filename):
    # load newspaper list as list
    with open(filename, "r") as f:
        newspaper_list = f.read().splitlines()
    return newspaper_list

def get_keys_from_metadata(metadata):
    keys = []
    for minibatch in metadata:
        keys += list(minibatch.keys())
    return keys

def extract_metadata_from_filename(filename):
    import re
    from datetime import datetime
    # Regular expression pattern
    pattern = r"(\d+)-(.+?)-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-(\d+)-(\d+)-p-(\d+)\.jpg"
    # Match the pattern
    match = re.match(pattern, filename)
    if match:
        # Parse date
        date_str = f"{match.group(5)}-{match.group(3)}-{match.group(4)}"
        date = datetime.strptime(date_str, "%Y-%b-%d")
        # Create a dictionary
        data = {
            "Id": match.group(1),
            "Newspaper": match.group(2).replace("-", " "),
            "Date": date.strftime("%Y-%m-%d"),  # formatted as YYYY-MM-DD
            "Pg": match.group(6)
        }
        # Print the dictionary
        for key, value in data.items():
            print(f"{key}: {value}")


test_metadata = load_metadata_from_manifest("all1973_size50_julyManifest")
test_keys = get_keys_from_metadata(test_metadata)
print(len(test_keys))
extract_metadata_from_filename(test_keys[0])
