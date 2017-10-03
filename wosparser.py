#!/usr/bin/python3

from add_zip import add_zip_file
from lxml import html
import mysql.connector as mariadb
import os
import re
import requests
import sys

def release_from_filename(filename):
    # Remove any trailing extensions we know about
    extensions = ('tzx', 'zip')
    while True:
        lastdot = filename.rfind('.')
        if lastdot == -1:
            break
        extension = filename[lastdot + 1:]
        if not extension in extensions:
            break
        filename = filename[:lastdot]

    release = 'Original'

    version = None
    match = re.match('^(.*)V([0-9](\.[0-9])?)$', filename)
    if match != None:
        (filename, version, _) = match.groups()

    box = None
    match = re.match('^(.*)\((Small|Large|Double)Case\)$', filename)
    if match != None:
        (filename, box) = match.groups()

    bugfix = False
    if filename.endswith('(BUGFIX)'):
        bugfix = True
        filename = filename[:-8]

    model = None
    match = re.match('^(.*)(48|128)$', filename)
    if match != None:
        (filename, model) = match.groups()

    if box != None:
        release += ', ' + box + ' Case'

    if model != None:
        release += ', ' + model + 'K'

    if version != None:
        release += ', V' + version

    if bugfix:
        release += ', Bugfix'

    return release

def get_tzx_release(tzx_filename, zip_release):
    tzx_filename = tzx_filename[:-4]

    match = re.match('^(.*) \(.*\)$', tzx_filename)
    if match != None:
        tzx_filename = match.groups()[0]

    side = None
    match = re.match('^(.*) - (Side [1-4AB])$', tzx_filename)
    if match != None:
        (tzx_filename, side) = match.groups()

    tape = None
    match = re.match('^(.*) (- )?(Tape [12])$', tzx_filename)
    if match != None:
        (tzx_filename, _, tape) = match.groups()

    machine = None
    match = re.match('^(.*) - ((48|128)[kK])$', tzx_filename)
    if match != None:
        (tzx_filename, machine, _) = match.groups()
        machine = machine.upper()
        if machine in zip_release:
            machine = None

    part = None
    match = re.match('^(.*) - (Part [12])$', tzx_filename)
    if match != None:
        (tzx_filename, part) = match.groups()

    players = None
    match = re.match('^(.*) - ([12] Player)s?$', tzx_filename)
    if match != None:
        (tzx_filename, players) = match.groups()

    release_strings = filter(lambda x: x != None, (tape, side, machine, part, players))
    tzx_release = ', '.join(release_strings)

    return tzx_release

if len(sys.argv) < 2:
    print('Please give an Infoseek ID')
    sys.exit(1)

infoseekid = int(sys.argv[1])
pageurl = 'http://www.worldofspectrum.org/infoseekid.cgi?id={:07d}'.format(infoseekid)
page = requests.get(pageurl)

tree = html.fromstring(page.content)
# Don't blame me for this XPath, WoS's html isn't exactly pretty ;-)
links = tree.xpath('//font[@size="+1"]/following-sibling::table/tr/td/font/a')
gamefragment = links[0].attrib['href']

if not gamefragment.endswith('.tzx.zip'):
    print('I got a link of {} which doesn\'t look right; bailing out'.format(gamefragment))
    sys.exit(1)

lastslash = gamefragment.rindex('/')
filename = gamefragment[(lastslash + 1)::]
gameurl = 'http://www.worldofspectrum.org' + gamefragment
game = requests.get(gameurl)

release = release_from_filename(filename)

with open(filename, 'xb') as gamefile:
    gamefile.write(game.content)

print('Wrote {} with release "{}"'.format(filename, release))

DATABASE = 'archive'
DATABASE_USER = 'philip'

database_password = os.environ['DATABASE_PASSWORD']

conn = mariadb.connect(database=DATABASE, user=DATABASE_USER, password=database_password)

add_zip_file(filename, conn, infoseekid, 'WoS', release, get_tzx_release)
