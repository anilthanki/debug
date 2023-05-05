import argparse
import logging
import json
import bioblend.galaxy
import bioblend.galaxy.libraries

from bioblend.galaxy.folders import FoldersClient
from datetime import date, datetime
import pytz
import os

utc = pytz.UTC

import requests

date_format = "%Y-%m-%d %H:%M:%S"

# set up logging
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
# set logging to output to stdout
logging.basicConfig()

# parse command line arguments with argparse
parser = argparse.ArgumentParser()

# Galaxy server
parser.add_argument("--server", help="Galaxy server")

# Galaxy API key
parser.add_argument("--api_key", help="Galaxy API key")

# dataset
parser.add_argument("--datasets_file", help="dataset file")

# source dir
parser.add_argument("--source_dir", help="Source directory path")

# Galaxy path
parser.add_argument("--galaxy_path", help="Galaxy path")


# list all datasets with their dates in s3 bucket
def list_datasets(datasets_file):
	datasets = {}
	with open(datasets_file, "r") as f:
		while line := f.readline():
			# split line into dataset name and date of last transfer
			log.debug(f"Reading line: {line}...")
			dataset_name, last_transfer_date = line.strip().split("\t")
			last_transfer_date = utc.localize(
				datetime.strptime(last_transfer_date, date_format)
			)

			if dataset_name.endswith('.7z'):
				datasets[dataset_name.replace(".7z", "")] = last_transfer_date
	return datasets

def get_libs(gi):
	libs = gi.libraries.get_libraries()
	return libs

def get_lib_by_name(gi, lib_name):
	lib = gi.libraries.get_libraries(name=lib_name)
	return lib


def create_new_lib(gi, lib_name, lib_desc, lib_synopsis):
	new_lib=gi.libraries.create_library(name=lib_name, description=lib_desc, synopsis=lib_synopsis)
	return new_lib

def upload_files_to_lib(gi, lib_id, source_dir, galaxy_path, root_folder):
	
	log.debug('upload_files_to_lib')
	
	folder_client = FoldersClient(gi)
	 
	root_folder = gi.libraries.get_libraries(lib_id)[0]['root_folder_id']
	for directory_path, directory_names, file_names in os.walk(source_dir):
		log.debug('main loop ..')
			
		# Recreate the directory structure
		for directory_name in directory_names:
			log.debug('directory_name %s', directory_name)
			log.debug('root_folder %s', root_folder)
			log.debug('directory_path %s', directory_path)

			if directory_path == source_dir:
				# Check if folder exist root 
				folder = gi.libraries.get_folders(lib_id, name="/"+directory_name)
				if folder == []:
					# Create folder in root 
					folder = gi.libraries.create_folder(lib_id, folder_name=directory_name)
			else:
				new_dirname = directory_path.replace(source_dir+"/", "")
				
				# Get the parent folder 
				parent_folder = gi.libraries.get_folders(lib_id, name="/"+new_dirname)
				
				log.debug('parent_folder %s', parent_folder)
				
				log.debug(new_dirname+"/"+directory_name)
				
				# Check if folder exist root 
				folder = gi.libraries.get_folders(lib_id, name="/"+new_dirname+"/"+directory_name)
				
				if folder == []:
					# Create folder within the parent folder 
					folder = gi.libraries.create_folder(lib_id, folder_name=directory_name, base_folder_id=parent_folder[0]['id'])
			
	
		for file_name in file_names:
			if not file_name.startswith('.'):
				log.debug('file_name %s', file_name)
				log.debug('directory_path %s', directory_path)
				log.debug('source_dir %s', source_dir)
				log.debug('galaxy_path %s', galaxy_path)
				
				if directory_path == source_dir:
					new_dirname = ""
				else:
					new_dirname = directory_path.replace(source_dir+"/", "")
				
				# Get the parent folder 
				folder = gi.libraries.get_folders(lib_id, name="/"+new_dirname)
				
				#show_folder = gi.libraries.show_folder(library_id=lib_id, folder_id=folder[0]['id'])
				
				show_folder = folder_client.show_folder(folder[0]['id'], contents=True)
				
				log.debug('show_folder %s', show_folder)
				
				files = {}
				
				for file in folder['folder_contents']:
					files[file['name']] = file['id']
				
				log.debug('files %s', files)
				
				if file_name in files:
					log.debug('%s present in %s', file, files)
					delete_library_dataset(library_id=lib_id, dataset_id=files[file], purged:True)
					
				file_path = os.path.join(galaxy_path, new_dirname, file_name)

				# Upload the file to the library
				gi.libraries.upload_from_galaxy_filesystem(lib_id, file_path, preserve_dirs=True, folder_id=folder[0]['id'])


def main():
	args = parser.parse_args()

	# check if all required arguments are provided
	if (
		not args.server
		or not args.api_key
		or not args.datasets_file
		or not args.source_dir
		or not args.galaxy_path
	):
		parser.print_help()
		sys.exit(1)

	server = args.server
	api_key = args.api_key

	datasets = list_datasets(args.datasets_file)

	gi = bioblend.galaxy.GalaxyInstance(url=server, key=api_key)
	
	for dataset_name in datasets:
		log.debug('dataset_name %s', dataset_name)
		lib = get_lib_by_name(gi, dataset_name)
		lib_id = 0
		if lib == []:
			lib = create_new_lib(gi, dataset_name, dataset_name, dataset_name)
			lib_id = lib['id']
		else:
			lib_id = lib[0]['id']

		root_folder = get_lib_by_name(gi, dataset_name)[0]['root_folder_id']
			
		upload_files_to_lib(gi, lib_id, args.source_dir, args.galaxy_path, root_folder)
		
		break
	

if __name__ == "__main__":
	main()
