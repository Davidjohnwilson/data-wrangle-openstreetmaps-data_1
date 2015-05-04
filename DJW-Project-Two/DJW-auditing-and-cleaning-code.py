#!/usr/bin/env python
# -*- coding: utf-8 -*-

#PROJECT TWO - OPENSTREETMAP DATA
#DATA WRANGLING WITH MONGODB
#DAVID JOHN WILSON

# Code for the auditing and cleaning of OSM data using python.

# Note: this code was created within an iPython notebook. Comment lines
#(as shown below) will separate what was previously separate notebook cells.
#Therefore this code is not intended to be run as a single file, but as
#clusters of code.

#---------

#Required packages

import xml.etree.ElementTree as ET
from pprint import pprint
import re
from collections import defaultdict
import codecs
import json
from pymongo import MongoClient

OSM_FILE = "san-francisco.osm"
SAMPLE_FILE = "sample-sf.osm"
CURRENT_FILE = OSM_FILE #Change this if you only want to use the sample file.

#---------

#We start by creating a sample OSM file using the provided code.

def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag

    Reference:
    http://stackoverflow.com/questions/3095434/inserting-newlines-in-xml-file-generated-via-xml-etree-elementtree-in-python
    """
    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


with open(SAMPLE_FILE, 'wb') as output:
    output.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    output.write('<osm>\n  ')

    # Write every 100th top level element
    for i, element in enumerate(get_element(OSM_FILE)):
        if i % 100 == 0:
            output.write(ET.tostring(element, encoding='utf-8'))

    output.write('</osm>')

#---------

#We now count the types of tags in the file.

def count_tags(filename):
        tags = {}

        for event,elem in ET.iterparse(filename):
            if elem.tag in tags:
                tags[elem.tag] += 1
            else:
                tags[elem.tag] = 1
        return tags

tags = count_tags(CURRENT_FILE)
pprint(tags)

#---------

#We analyse the types of keys to check primarily for any problem characters.

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')


def key_type(element, keys):
    if element.tag == "tag":
        key = element.attrib['k']
        if lower.match(key):
            keys["lower"] = keys["lower"] + 1
        elif lower_colon.match(key):
            keys["lower_colon"] = keys["lower_colon"] + 1
        elif problemchars.match(key):
            keys["problemchars"] = keys["problemchars"] + 1
        else:
            keys["other"] = keys["other"] + 1
    return keys



def process_map(filename):
    keys = {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0}
    for _, element in ET.iterparse(filename):
        keys = key_type(element, keys)

    return keys

keys = process_map(CURRENT_FILE)
pprint(keys)

# If the number of problemchars is greater than 0, they should be dealt with by either ignoring the entry, or removing the problem character.

#---------

#One of the most difficult fields to clean is the street name, which has many different kinds of abbreviations. The following code audits the street types.

street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE) #final word

expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road", "Trail", "Parkway", "Commons"]

def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)


def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")

#This function checks all street names in the file.
def audit(osmfile):
    osm_file = open(osmfile, "r")
    street_types = defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):
        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])
    return street_types

#The following code gives the standardized form of the street.
def update_name(name, mapping):
    name_end = name.split(' ')[-1]
    for k in mapping:
        if name_end == k:
            name = name.replace(k,mapping[k])
    return str(name+"")

audit_output = audit(CURRENT_FILE)

#The result is a defaultdict so we pprint each entry
for aud in audit_output:
    pprint([aud,audit_output[aud]])

#---------

#The following dictionary consists of all the entries that need to be corrected, in the format "old_string : new_string". These were manually created from the results of the audit

mapping = { "St": "Street",
            "St.": "Street",
            "Ave": "Avenue",
            "Ave.": "Avenue",
            "Abenue": "Avenue",
            "Avenie": "Avenue",
            "Rd": "Road",
            "Rd.": "Road",
            "Pl" : "Plaza",
            "Pl." : "Plaza",
            "broadway": "Broadway",
            "street": "Street",
            "Plz" : "Plaza",
            "Blvd": "Boulevard",
            "Blvd.": "Boulevard",
            "Boulavard": "Boulevard",
            "square": "Square",
            "parkway": "Parkway",
            "ave": "Avenue",
            "Ln": "Lane",
            "Hwy": "Highway",
            "Dr": "Drive",
            "Ctr": "Center",
            "sutter": "Sutter",
            "Ln.": "Lane",
            "st": "Street"
            }

#---------

#We now give functions to shape the data appropriately for input into MongoDB.

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

CREATED = [ "version", "changeset", "timestamp", "user", "uid"] #Fields for 'created'
LONGLAT = [ "lon", "lat" ] #Fields for 'pos'


#This function takes a single element and produces a dictionary with all the information for that element in a standardized structure.
def shape_element(element):
    node = {}
    if element.tag == "node" or element.tag == "way" :
        node['type'] = element.tag
        #Initialize the entries that will have multiple componenets
        node['created'] = {}
        node['pos'] = [0,0]
        node['address'] = {}
        if element.tag == 'way':
            node['node_refs'] = []

        #Start adding attributes
        for k in element.attrib:
            if k in CREATED:
                node['created'][k] = element.attrib[k]
            elif k in LONGLAT:
                #The following if/else completes a sanity check on the
                #longitude/latitude and decides which position in the
                #'pos' array the value should go.
                if k == 'lat':
                    if float(element.attrib[k]) > 38 or float(element.attrib[k]) < 37:
                        #Sanity check on the latitude
                        print "Incorrect latitude: " + str(element.attrib[k])
                    long_lat = 0
                else:
                    if float(element.attrib[k]) > -122 or float(element.attrib[k]) < -123:
                        #Sanity check on the longitude
                        print "Incorrect longitude: " + str(element.attrib[k])
                    long_lat = 1
                node['pos'][long_lat] = float(element.attrib[k])
            elif k == 'address':
                #The following code was added due to an entry having an
                #'address' tag which interfered with the component entry of
                #'address'. We reinitialize to be safe.
                print 'Found an address!'
                print element
                node['address'] = {}
            else:
                node[k] = element.attrib[k]

        #Initialize the state and country for all address entries.
        node['address'] = {'state':'CA', 'country':'US'}

        for child in element:
            if child.tag == 'tag':
                if problemchars.match(child.attrib['k']) or problemchars.match(child.attrib['v']) or child.attrib['k'] == 'address':
                    #Skip any entries with problem characters in the key
                    #or value. Also skip any with a key 'address' as this
                    #interfereswith the component 'address' from 'addr' entries.
                    next

                #Subkeys are split with a ':'
                key_array = child.attrib['k'].split(':')
                if key_array[0] == 'addr':
                    if len(key_array) > 1 and key_array[1] == 'street':
                        if not child.attrib['v'] != u'Ca\xf1ada Road':
                            #We don't want to deal with this unicode character
                            #in our update so we deal separately.
                            node['address']['street'] = str(update_name(child.attrib['v'],mapping))
                        else:
                            node['address']['street'] = child.attrib['v']
                    elif key_array[1] == 'state':
                        if child.attrib['v'] != 'CA':
                            #Warn about non-valid states.
                            #(We therefore do not alter the entry in 'address')
                            print "Non valid state: " + str(child.attrib['v'])
                    elif key_array[1] == 'country':
                        if child.attrib['v'] != 'US':
                            #Warn about non-valid countries.
                            #(We therefore do not alter the entry in 'address')
                            print "Non valid country: " + str(child.attrib['v'])
                    elif key_array[1] == 'postcode' and len(child.attrib['v'])>5:
                        #We warn and deal with postcodes that are over 5 chars.
                        print "Too long postcode: " + str(child.attrib['v'])
                        #We remove any CA prefix, and take only the numbers
                        #before any hyphen. Finally, we force ourselves to only
                        #the first 5 characters.
                        node['address'][key_array[1]] = child.attrib['v'].replace("CA ","").split('-')[0][:5]
                    else:
                        node['address'][key_array[1]] = child.attrib['v']
                else:
                    node[child.attrib['k']] = child.attrib['v']
            elif child.tag == 'nd':
                #For node references we append to the array
                node['node_refs'].append(child.attrib['ref'])

        if len(node['address']) == 0:
            #If any address is empty, we delete the 'address' entry.
            #Note, this is not needed now we are setting the State/Country.
            del node['address']

        return node
    else:
        return None


def process_map(file_in, pretty = False):
    file_out = "{0}.json".format(file_in)
    data = []
    with codecs.open(file_out, "w", encoding='utf-8') as fo:
        for _, element in ET.iterparse(file_in):
            el = shape_element(element)
            if el:
                data.append(el)
                if pretty:
                    fo.write(json.dumps(el, indent=2)+"\n")
                else:
                    fo.write(json.dumps(el) + "\n")
    return data

#---------

#We process our data and look at the first 5 entries.

data = process_map(CURRENT_FILE, False)

print data[:5]

#---------

#We now start a MongoDB server locally, and import our data into
# database:    cities
# collection:  sanfrancisco
#
# > mongoimport --db cities --collection sanfrancisco --file= san-francisco_california.osm.json

#We set up the pymongo client.

from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')

#---------

#We select our database: cities. We can then call db.sanfrancisco to get our
#collection.

db = client.cities

#---------

#We can now run any database queries using calls to db. These are described in full in the DJW-Project-Two.pdf file, but below are some examples.

def print_results(collection):
    for c in collection['result']:
        print str(c['_id']) + ": " + str(c['count'])

#---------

#The total number of entries in our database.
print "Total number of entries: " + str(db.sanfrancisco.find().count())

#---------

#The top five contributing users.

users = db.sanfrancisco.aggregate([{"$group":{"_id":"$created.user", "count":{"$sum":1}}},
                                   {"$sort":{"count":-1}},
                                   {"$limit":5}])
print "Top Contributing Users: "
print_results(users)

#---------

#The top five fast food chains in SF.

fast_food = db.sanfrancisco.aggregate([{"$match":{"amenity":{"$exists":1}, "amenity":"fast_food"}},
{"$group":{"_id":"$name", "count":{"$sum":1}}},
{"$sort":{"count":-1}}, {"$limit":5}])


print "Fast Food Chains:"
print_results(fast_food)

#---------

#Comparing the number of toilets and water fountains in SF.

toilets = db.sanfrancisco.aggregate([
{"$match":{"amenity":{"$exists":1}, "amenity":"toilets"}},
{"$group":{"_id":"dummystring", "count":{"$sum":1}}}])

fountains = db.sanfrancisco.aggregate([
{"$match":{"amenity":{"$exists":1}, "amenity":"drinking_water"}},
{"$group":{"_id":"dummystring", "count":{"$sum":1}}}])

print "Number of public toilets: " + str(toilets['result'][0]['count'])
print "Number of drinking fountains: " + str(fountains['result'][0]['count'])

#---------

#Finding out if there are more museums than Starbucks + McDonald's in SF.

fast_food = db.sanfrancisco.aggregate([
{"$match":{"name":{"$in":['Starbucks',"McDonald's"]}}},
{"$group":{"_id":"dummystring", "count":{"$sum":1}}}])

museums = db.sanfrancisco.aggregate([
{"$match":{"tourism":"museum"}},
{"$group":{"_id":"dummystring", "count":{"$sum":1}}}])

print "Number of Starbucks + McDonald's: " + str(fast_food['result'][0]['count'])
print "Number of Museums: " + str(museums['result'][0]['count'])

#---------

#Code by David John Wilson, adapted from the Udacity 'Data Wrangling with
#MongoDB' course.
