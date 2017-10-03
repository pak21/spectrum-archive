#!/usr/bin/python3

import contextlib
import glob
import hashlib
import mysql.connector as mariadb
import os
import sys
import zipfile

def find_unused_filename(filename):
    yield filename
    base = filename[:-4]
    n = 1
    while True:
        yield '{}-{}.tzx'.format(base, n)
        n += 1

def ask_for_tzx_release(filename, zip_release):
    print('Release info for tzx file {}?'.format(filename))
    return input('> ')

def add_zip_file(filename, conn, zxdb_id, source, zip_release, tzx_detail_callback):
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
            if (zi.filename.endswith('.tzx')):
                tzx_release = tzx_detail_callback(zi.filename, zip_release)
                print('Creating TZX file {} with release "{}"'.format(zi.filename, tzx_release))
                with zip.open(zi) as tzxfile:
                    tzx_data = tzxfile.read()
                tzx_sha256 = hashlib.sha256(tzx_data).hexdigest()
                for newfilename in find_unused_filename('tzx/{}'.format(zi.filename)):
                    try:
                        with open(newfilename, 'xb') as newfile:
                            newfile.write(tzx_data)
                        basename = newfilename[4::]
                        with contextlib.closing(conn.cursor()) as cursor:
                            cursor.execute('INSERT INTO tzxs SET release_id = %s, details = %s, sha256 = %s, filename = %s', (zip_id, tzx_release, tzx_sha256, basename))
                            conn.commit()
                        break
                    except:
                        # Try next filename
                        pass

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

    add_zip_file(filename, conn, zxdb_id, source, zip_release, ask_for_tzx_release)

if __name__ == '__main__':
    main()
