#!/usr/bin/python3

import contextlib
import glob
import hashlib
import mysql.connector as mariadb
import os
import sys
import zipfile

def find_unused_filename(filename, extension):
    yield filename
    base = filename[:-4]
    n = 1
    while True:
        yield '{}-{}.{}'.format(base, n, extension)
        n += 1

def ask_for_tzx_details(filename, zip_release):
    print('Details for tzx file {}?'.format(filename))
    return input('> ')

def ask_for_trd_details(filename, zip_release):
    print('Details for trd file {}?'.format(filename))
    return input('> ')

def add_zip_file(filename, conn, zxdb_id, source, zip_release, detail_callbacks):
    with open(filename, 'rb') as zip2:
        zip_data = zip2.read()
    zip_sha256 = hashlib.sha256(zip_data).hexdigest()
    with contextlib.closing(conn.cursor()) as cursor:
        cursor.execute('SELECT id, `release` FROM releases WHERE sha256 = %s', (zip_sha256,))
        rows = cursor.fetchall()
    if len(rows) > 0:
        (zip_id, release) = rows[0]
        print('{} already exists with ID {}'.format(filename, zip_id))
        sys.exit(1)

    with contextlib.closing(conn.cursor()) as cursor:
        cursor.execute('INSERT INTO releases SET zxdb_id = %s, source = %s, `release` = %s, sha256 = %s, filename = %s', (zxdb_id, source, zip_release, zip_sha256, filename))
        cursor.execute('SELECT LAST_INSERT_ID()')
        (zip_id,) = (cursor.fetchall())[0]
        conn.commit()

    with zipfile.ZipFile(filename) as zip:
        for zi in zip.infolist():
            if (zi.filename.endswith('.tzx') or zi.filename.endswith('.trd')):
                extension = zi.filename[-3:]
                details = detail_callbacks[extension](zi.filename, zip_release)
                print('Creating {} file {} with details "{}"'.format(extension, zi.filename, details))
                with zip.open(zi) as datafile:
                    data = datafile.read()
                data_sha256 = hashlib.sha256(data).hexdigest()
                for newfilename in find_unused_filename('files/{}'.format(zi.filename), extension):
                    try:
                        with open(newfilename, 'xb') as newfile:
                            newfile.write(data)
                        basename = newfilename[6::]
                    except:
                        # Try next filename
                        continue
                    
                    with contextlib.closing(conn.cursor()) as cursor:
                        cursor.execute('INSERT INTO files SET release_id = %s, type = %s, details = %s, sha256 = %s, filename = %s', (zip_id, extension, details, data_sha256, basename))
                        conn.commit()
                    break
            else:
                print('Skipping unknown file {}'.format(zi.filename))

def main():
    DATABASE = 'archive'
    DATABASE_USER = 'philip'

    database_password = os.environ['DATABASE_PASSWORD']

    conn = mariadb.connect(database=DATABASE, user=DATABASE_USER, password=database_password)

    if len(sys.argv) < 2:
        print('Please give a zip file name')
        sys.exit(1)

    filename = sys.argv[1]

    print('ZXDB ID for zip file {}?'.format(filename))
    zxdb_id = input('> ')
    print('Source for zip file {}?'.format(filename))
    source = input('> ')
    print('Release info for zip file {}?'.format(filename))
    zip_release = input('> ')

    add_zip_file(filename, conn, zxdb_id, source, zip_release, {'tzx': ask_for_tzx_details, 'trd': ask_for_trd_details})

if __name__ == '__main__':
    main()
