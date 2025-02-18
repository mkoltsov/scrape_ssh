#!/usr/bin/env python3

import sys
import yaml
import os
import subprocess
from atlassian import Confluence
import paramiko
import re
import logging
import paramiko.util
import socket
import time
import select
from functools import wraps
from paramiko.proxy import ProxyCommand
import pandas as pd
from bs4 import BeautifulSoup

def read_config():
    """Read and parse the config.yaml file"""
    try:
        with open('config.yaml', 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        print("Error: config.yaml file not found")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error parsing config.yaml: {e}")
        sys.exit(1)

def print_config(config):
    """Print the configuration values"""
    print(f"Confluence URL: {config['confluence']['url']}")
    print(f"Page ID: {config['confluence']['page_id']}")
    print(f"Hostname Column: {config['confluence']['hostname_column_title']}")
    print(f"IP Column: {config['confluence']['ip_column_title']}")

def authenticate_confluence(confluence_url):
    """Authenticate to Confluence using environment variables"""
    okta_user = os.getenv('OKTA_USER')
    okta_password = os.getenv('OKTA_PASSWORD')

    if not okta_user or not okta_password:
        print("Error: OKTA_USER or OKTA_PASSWORD environment variables not set")
        sys.exit(1)

    try:
        confluence = Confluence(
            url=confluence_url,
            username=okta_user,
            password=okta_password
        )
        return confluence
    except Exception as e:
        print(f"Error authenticating to Confluence: {e}")
        sys.exit(1)

def get_page_content(confluence, page_id):
    """Get the content of the Confluence page"""
    try:
        return confluence.get_page_by_id(page_id, expand='body.storage')
    except Exception as e:
        print(f"Error retrieving page content: {e}")
        sys.exit(1)

def find_server_ip(html_content, server_name, hostname_column, ip_column):
    """Find the IP address for the given server name using pandas"""
    try:
        # Parse HTML using BeautifulSoup to get all tables
        soup = BeautifulSoup(html_content, 'html.parser')
        tables = soup.find_all('table')

        # Convert each table to pandas DataFrame and search for the server
        for table in tables:
            # Read HTML table into pandas DataFrame
            df = pd.read_html(str(table))[0]

            # Clean up column names (remove any whitespace)
            df.columns = df.columns.str.strip()

            # Try to find the server name in the hostname column
            try:
                # Filter the DataFrame for the server name
                server_row = df[df[hostname_column].str.strip() == server_name]

                if not server_row.empty:
                    # Get the IP address from the matching row
                    ip_address = server_row[ip_column].iloc[0]
                    return str(ip_address).strip()
            except KeyError:
                # Column names don't match, try next table
                continue

        return None

    except Exception as e:
        print(f"Error parsing table data: {e}")
        return None

def validate_config(config):
    """Validate the configuration file has all required fields"""
    required_fields = ['url', 'page_id', 'hostname_column_title', 'ip_column_title']

    if 'confluence' not in config:
        print("Error: 'confluence' section missing in config.yaml")
        sys.exit(1)

    for field in required_fields:
        if field not in config['confluence']:
            print(f"Error: '{field}' missing in confluence configuration")
            sys.exit(1)

def main():
    # Check command line arguments
    if len(sys.argv) != 2:
        print("Usage: python script.py <server-name>")
        sys.exit(1)

    server_name = sys.argv[1]

    # Read configuration
    config = read_config()

    # Validate configuration
    validate_config(config)

    # Print configuration
    print_config(config)

    # Get Confluence configuration
    confluence_config = config['confluence']
    confluence_url = confluence_config['url']
    page_id = confluence_config['page_id']
    hostname_column = confluence_config['hostname_column_title']
    ip_column = confluence_config['ip_column_title']

    # Authenticate and get page content
    confluence = authenticate_confluence(confluence_url)
    page = get_page_content(confluence, page_id)

    # Find IP address
    ip_address = find_server_ip(
        page['body']['storage']['value'],
        server_name,
        hostname_column,
        ip_column
    )

    if not ip_address:
        print(f"Server '{server_name}' not found")
        sys.exit(1)

    print(f"IP Address for {server_name}: {ip_address}")

    # Create SSH session

    command = f"""while ! ssh {ip_address}; do
      echo "Connection failed, retrying in 5 seconds..."
      sleep 5
    done
    echo "Connection established."""  # Replace with the desired command

    # Open a bash session, execute the command, and give control to bash
    process = subprocess.run(
        ['/bin/bash', '-i', '-c', f'{command}; exec bash'],
        shell=False,
        executable='/bin/bash'
    )

if __name__ == "__main__":
    main()
