"""********************************************
* A sample Python script for creating / updating layers
* from Dataminr API responses.

Open questions:
- Should this be a push vs a bulk update (if the latter, how to paginate)?
- Are alertIds unique? s.t. we can query all alerts and ignore those that exist
- What's the relationship between lists and alerts?
  - Will there be overlapping alerts between lists?
  - What should list field values be for an alerts feature?
********************************************"""

import datetime
import logging
import os
import tempfile
import json
import shutil
import requests

from arcgis.gis import GIS, Item # https://developers.arcgis.com/python/

"""********************************************
* Utility functions
********************************************"""

def extract(obj, keys, **kwargs):
    """returns a nested object value for the specified keys"""
    required = kwargs.pop('required', False)
    default = kwargs.pop('default', None)
    warn = kwargs.pop('warn', False)
    
    o = obj
    for i in range(0, len(keys)):
        try:
            o = o[keys[i]]
        except (KeyError, IndexError):
            if warn:
                print('Warning key does not exist. Key: {0} in Keys: {1}'.format(keys[i], keys))
            if required and default == None:
                raise KeyError('Required key does not exist in object and no default')
            return default
    return o

def d_extract(obj, keys_delimited, **kwargs):
    """returns a nested object value for delimited keys"""
    keys = keys_delimited.split('.')
    return extract(obj, keys, **kwargs)

def row_to_geojson(row, lon_field, lat_field):
    """returns a geojson feature for a flat dictionary row"""
    return {
        'type': 'Feature',
        'geometry': {
            'type': 'Point',
            'coordinates': [row[lat_field], row[lon_field]]
        },
        'properties': {**row}
    }

def rows_to_geojson(rows, lon_field, lat_field):
    """returns a geojson feature collection for a list of flat dictionary rows"""
    features = [row_to_geojson(r, lon_field, lat_field) for r in rows]
    return {
        'type': 'FeatureCollection',
        'features': features
    }

def date_to_ags(date):
    """Returns an ArcGIS-formatted date from a Python date object"""
    tz = datetime.timezone.utc
    return date.astimezone(tz).strftime('%m/%d/%Y %H:%M:%S')

def timestamp_to_ags(timestamp):
    """Returns an ArcGIS-formatted date from a ms timestamp"""
    seconds = timestamp / 1000
    date = datetime.datetime.fromtimestamp(seconds)
    return date_to_ags(date)

"""********************************************
* Dataminr API response parsing functions
********************************************"""

def alert_to_row(obj):
    """returns a flat dictionary row parsed from a dataminr alert object"""
    f_e = lambda keys, **kwargs: extract(obj, keys, warn=False, **kwargs)
    f_de = lambda keys, **kwargs: d_extract(obj, keys, warn=False, **kwargs)
    
    # simple JSON parsed values
    props = {
        'alert_id': f_de('alertId', required=True), #hardcoded later to identify alerts
        'place': f_de('eventLocation.name'),
        'alert_type': f_de('alertType.name'),
        'alert_type_color': f_de('alertType.color'),
        'caption': f_de('caption'),
        'publisher_category': f_de('publisherCategory.name'),
        'publisher_category_color': f_de('publisherCategory.color'),
        'related_terms_query_url': f_de('relatedTermsQueryURL'),
        'expand_alert_url': f_de('expandAlertURL'),
        'post_text': f_de('post.text'),
        'post_text_transl': f_de('post.translatedText'),
        'lon': f_e(['eventLocation','coordinates',0]),
        'lat': f_e(['eventLocation','coordinates',1])
    }
    
    # JSON parsed values with manipulations
    event_time = f_de('eventTime') # hardcoded later to delete old events
    if event_time and event_time > 0:
        props['event_time'] = timestamp_to_ags(event_time)
        
    post_time = f_de('post.timestamp')
    if post_time and post_time > 0:
        props['post_time'] = timestamp_to_ags(post_time)
        
    channels = f_de('source.channels')
    if channels:
        props['source'] = ','.join(channels)
        
    terms = f_de('relatedTerms')
    if terms:
        props['related_terms'] = ','.join([t['text'] for t in terms])
        
    categories = f_de('categories')
    if categories:
        props['categories'] = ','.join([c['name'] for c in categories])
        
    return props

def list_to_row(obj):
    """returns a flat dictionary parsed from a dataminr list object"""
    f_de = lambda keys, **kwargs: d_extract(obj, keys, warn=True, **kwargs)
    return {
        'list_id': f_de('id', required=True),
        'list_name': f_de('name'),
        'list_color': f_de('properties.watchlistColor')
    }

"""********************************************
* Dataminr API wrappers
********************************************"""

def get_auth_header(client_id, client_secret):
    params = {'grant_type': 'api_key', 'client_id': client_id, 'client_secret': client_secret}
    r = requests.post('https://gateway.dataminr.com/auth/2/token', params)
    j = r.json()
    return {'Authorization': 'dmauth {0}'.format(j['dmaToken'])}

def get_lists(headers):
    r = requests.get('https://gateway.dataminr.com/account/2/get_lists', headers=headers)
    j = r.json()
    topics = d_extract(j, 'watchlists.TOPIC', default=[])
    companies = d_extract(j, 'watchlists.COMPANY', default=[])
    custom = d_extract(j, 'watchlists.CUSTOM', default=[])
    lists = topics + companies + custom
    return [list_to_row(l) for l in lists]

def get_alerts(headers, list_ids, **kwargs):
    pagesize = kwargs.pop('pagesize', 100)
    params = {'alertversion': 14, 'lists': list_ids, 'pagesize': pagesize, **kwargs}
    r = requests.get('https://gateway.dataminr.com/alerts/2/get_alert', params=params, headers=headers)
    alerts = r.json()
    return [alert_to_row(a) for a in alerts]

"""********************************************
* ArcGIS functions
********************************************"""

def add_geojson(gis, geojson, **item_options):
    """Uploads geojson and returns the file item"""
    # get default args
    title = item_options.pop('title', 'Dataminr Sample')
    tags = item_options.pop('tags', 'dataminr-poc')
        
    # save geojson to tempfile and add as item
    with tempfile.NamedTemporaryFile(mode="w", suffix='.geojson') as fp:
        fp.write(json.dumps(geojson))
        item = gis.content.add({
            **item_options,
            'type': 'GeoJson',
            'title': title,
            'tags': tags,
        }, data=fp.name)
    
    return item

def create_scratch_layer(gis, geojson, **item_options):
    """Publishes parsed dataminr geojson as a service and returns the resulting layer item
    
    Note, use this to quickly add geojson with system default properties. In production,
    it's easier to set desired properties on a template layer then use create_layer."""

    item = add_geojson(gis, geojson, **item_options)
    try:
        lyr_item = item.publish()
    except Exception as e:
        item.delete()
        logging.error('Error creating a new layer: {0}'.format(str(e)))
        return
    item.delete() # if not deleted next run will eror
    
    # add a unique index for upsert operations
    new_index = {
        "name" : "Alert ID", 
        "fields" : "alert_id",
        "isUnique" : True,
        "description" : "Unique alert index" 
    }
    add_dict = {"indexes" : [new_index]}
    lyr = lyr_item.layers[0]
    lyr.manager.add_to_definition(add_dict)
    
    return lyr_item

def create_layer(gis, geojson, template_item):
    """Publishes parsed dataminr geojson as a service based on an existing
    template item and returns the resulting layer item"""

    try:
        results = gis.content.clone_items([template_item], copy_data=False)
        item = results[0]
        lyr = item.layers[0]
    except Exception as e:
        logging.error('Error creating a new layer from template: {0}'.format(str(e)))

    return append_to_layer(gis, lyr, geojson)

def append_to_layer(gis, layer, geojson):
    """Appends parsed dataminr geojson to an existing service
    
    Note, this is the best approach for bulk updates in ArcGIS Online.
    There are other options here, such as transactional edits
    > https://github.com/mpayson/esri-partner-tools/blob/master/feature_layers/update_data.ipynb
    """

    item = add_geojson(gis, geojson, title="Dataminr update")
    result = layer
    test = item.id
    try:
        result = layer.append(
            item_id=test,
            upload_format="geojson",
            #upsert_matching_field="alert_id"
        )
        print(item)
        print(result)
    except Exception as e:
        logging.error('Error appending data to existing layer: {0}'.format(str(e)))
    finally:
      item.delete() # if not deleted next run will eror
    
    return result

def delete_before(lyr, date, field):
    """Deletes all features in a layer before a given date"""
    where = "{0} < '{1}'".format(field, date_to_ags(date))
    return lyr.delete_features(where=where)

def delete_before_days(lyr, number_days, field):
    """Deletes all features with dates before the specified
    number of days back from today"""
    dt = datetime.datetime.today() - datetime.timedelta(number_days)
    return delete_before(lyr, dt, field)


"""********************************************
* The main show
********************************************"""

def run(gis_un, gis_pw, client_id, client_secret):

    # if user has previously signed in to system, can also construct with token
    # > gis = GIS(token="<access token>")
    logging.info('Authenticating to GIS and Dataminr')
    gis = GIS(username=gis_un, password=gis_pw)
    headers = get_auth_header(client_id, client_secret)

    # get alerts for each list, note alert ids need to be unique so only 
    # use the alert the first time it is returned from a list request
    # TODO is this the best approach?
    logging.info('Getting Dataminr data')
    lists = get_lists(headers)
    alerts = []
    alert_ids = set()
    for l in lists:
        new_alerts = get_alerts(headers, str(l['list_id']), pagesize=100)
        for a in new_alerts:
            if a['alert_id'] in alert_ids:
                continue
            alerts.append({**a, **l})
            alert_ids.add(a['alert_id'])
    geojson = rows_to_geojson(alerts, 'lon', 'lat')

    # check to see if a layer already exists, if so update, else create
    # can alternatively save layer item ids to a store then reference
    item = Item(gis, 'cc6c9d08bc814274a9e39439c0d1a21d')
    lyr = item.layers[0]
    delete_before_days(lyr,30,'event_time') #delete old features
    logging.info('Updating existing layer {0} with {1} alerts'.format(item.id, len(alerts)))
    append_to_layer(gis, lyr, geojson)
    
    #search_items = gis.content.search('title:"test" AND type:"Feature Service"')
    #if len(search_items) > 0:
    #    item = search_items[0]
    #    lyr = item.layers[0]

    #    delete_before_days(lyr, 30, 'event_time') # delete old features

    #    logging.info('Updating existing layer {0} with {1} alerts'.format(item.id, len(alerts)))
    #    append_to_layer(gis, lyr, geojson)

    #else:
    #logging.info('Creating a new layer with {0} alerts'.format(len(alerts)))
    #create_scratch_layer(gis, geojson, title="Supply Chain Demo", tags="dataminr-poc")

        # can alternatively create a layer from an existing item used as a template:
    #template_item = Item(gis, 'fcd1dad0687741ae87bac9966fa727e1')
    #item = create_layer(gis, geojson, template_item)
        # here, the gis parameter should reference the account where the item lives
        # in this case, the accounts are the same so the gis is the same

    logging.info('Seemingly, a success')

if __name__ == "__main__":
    
    logging.getLogger().setLevel(logging.INFO)

    ags_un = 'mliss_dataminr1'
    ags_pw = '9IIwSp1!f1Hf'
    dm_id = 'f2c34fe6e15f4537ab3312af9ce9f11f'
    dm_se = 'eb106dfd83df4fb09331d08496c1b174'

    run(ags_un, ags_pw, dm_id, dm_se)
