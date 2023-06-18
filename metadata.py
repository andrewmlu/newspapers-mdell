import json

FILE_PATH = "/mnt/data01/pipeline_backup/pipeline_egress/"

def load_metadata_from_minibatch(minibatch_path):
    # read from .ocr_text.json
    with open(minibatch_path + "ocr_text.json", "r") as f:
        metadata = json.load(f)
    return metadata

def list_all_subdirs(dir_path):
    import os
    return [name for name in os.listdir(dir_path) if os.path.isdir(os.path.join(dir_path, name))]

def load_metadata_from_manifest(manifest_name):

    # read each mini_batch from each directory labeled mini_batch_xxx and pass into load_metadata_from_minibatch

    # list all directories in manifest_path
    subdirs = list_all_subdirs(FILE_PATH + manifest_name)

    # load metadata from each mini_batch
    metadata_paths = []
    for subdir in subdirs:
        metadata_paths.append(load_metadata_from_minibatch(FILE_PATH + manifest_name + "/" + subdir + "/"))

    metadata = []
    for metadata_path in metadata_paths:
        metadata.append(load_metadata_from_minibatch(metadata_path))

    return metadata

print(load_metadata_from_manifest("all1973_size50_julyManifest")[4])


