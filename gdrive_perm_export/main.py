import time
from csv import DictWriter

from apiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
import click
from more_itertools import first


SCOPES = 'https://www.googleapis.com/auth/drive.metadata.readonly'
MIME_TYPE_FOLDER = 'application/vnd.google-apps.folder'

PERM_TYPE_ATTR_MAP = {
    'group': lambda p: f"'{p.get('displayName', '')}' <{p['emailAddress']}>",
    'user': lambda p: f"'{p.get('displayName', '')}' <{p['emailAddress']}>",
    'anyone': lambda p: '[Anyone with link]',
    'domain': lambda p: p['domain'],
}


@click.command(help='Exports owners and writers of all files below the given folder id.')
@click.option('-o', '--output-file', type=click.File('w'), default='-')
@click.argument('ROOT-FOLDER-ID')
def main(root_folder_id, output_file):
    store = file.Storage('credentials.json')
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('client_secret.json', SCOPES)
        creds = tools.run_flow(flow, store)
    service = build('drive', 'v3', http=creds.authorize(Http()))

    seen_folder_ids = set()
    folder_ids = [(root_folder_id, '')]
    files = []

    print()
    while folder_ids:
        folder_id, path = folder_ids.pop()
        if folder_id in seen_folder_ids:
            continue
        seen_folder_ids.add(folder_id)
        print(
            f'\r'
            f'Folders: {len(seen_folder_ids)}, '
            f'Files: {len(files)}, '
            f'Now fetching {path} ({folder_id})',
            end=''
        )
        result = service.files().list(
            q=f"'{folder_id}' in parents",
            fields="nextPageToken, files(id, name, mimeType, permissions)"
        ).execute()
        for file_info in result['files']:
            if file_info['mimeType'] == MIME_TYPE_FOLDER:
                folder_ids.append((file_info['id'], f"{path}/{file_info['name']}"))
                continue
            permissions = file_info.get('permissions')
            owner = ''
            writers = ''
            if permissions:
                owner = first(
                    PERM_TYPE_ATTR_MAP[perm['type']](perm)
                    for perm in permissions
                    if perm['role'] == 'owner'
                )
                writers = ','.join(
                    PERM_TYPE_ATTR_MAP[perm['type']](perm)
                    for perm in permissions
                    if perm['role'] == 'writer'
                )
            files.append({
                'id': file_info['id'],
                'name': file_info['name'],
                'path': path,
                'owner': owner,
                'writers': writers
            })
        time.sleep(.125)
    print()
    writer = DictWriter(output_file, ['id', 'name', 'path', 'owner', 'writers'])
    writer.writeheader()
    writer.writerows(files)


if __name__ == "__main__":
    main()
