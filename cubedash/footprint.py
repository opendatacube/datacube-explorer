# coding: utf-8

# In[28]:


import math
import os
import shutil
import sys
import time
from datetime import datetime, timedelta

import ephem
import folium
import geopandas
import osgeo.ogr
import osgeo.osr
import requests
import simplekml
from geographiclib.geodesic import Geodesic
from IPython.display import display
from osgeo import ogr
from pyorbital import tlefile
from pyorbital.orbital import Orbital
from shapely.geometry import mapping

import wget

get_ipython().magic("matplotlib inline")


# In[2]:


# Check for correct usage
# if len(sys.argv)<3:
#    print "*--------------------------------------------------------------------*"
#    print ""
#    print " tle_predict_lat_lon.py computes current position, observer track, "
#    print " and approximate imaging footprint of Earth Observation Satellites "
#    print ""
#    print "*--------------------------------------------------------------------*"
#    print ""
#    print " usage: tle_predict_lat_lon.py <period to predict(mins)> <output path> "
#    print ""
#    print "*--------------------------------------------------------------------*"
# sys.exit()


# In[52]:


# Read arguments

ground_station = ("-23 42", "133 54")  # Alice Spring Data Acquisition Facility
period = (
    288_000
)  # int(sys.argv[1]) # Generate passes for this time period from start time
output_path = "."  # sys.argv[2]
if not os.path.exists(output_path):
    print "OUTPUT PATH DOESN'T EXIST", output_path
    sys.exit()
sleep_status = 1  # how many minutes to sleep between status updates
schedule = []
# Earth parameters for heading calculations
one_on_f = 298.257_223_563  # Inverse flattening 1/f = 298.257223563
f = 1 / one_on_f  # flattening
r = 6_378_137


# In[4]:


def download_file(url):
    local_filename = url.split("/")[-1]
    r = requests.get(url, stream=True)
    with open(local_filename, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
    return local_filename


# In[5]:


def get_tles():

    # GetTLEs(): returns a list of tuples of kepler parameters for each satellite.
    resource = "http://www.celestrak.com/norad/elements/resource.txt"
    weather = "http://www.celestrak.com/norad/elements/weather.txt"

    try:
        os.remove("resource.txt")
    except OSError:
        pass
    try:
        os.remove("weather.txt")
    except OSError:
        pass
    try:
        os.remove("stations.txt")
    except OSError:
        pass

    try:
        download_file(resource)
    except OSError:
        print "COULD NOT DOWNLOAD resource.txt"
        return ()

    try:
        download_file(weather)

    except OSError:
        print "COULD NOT DOWNLOAD weather.txt"
        return ()

    file_names = ["weather.txt", "resource.txt"]
    with open("tles.txt", "w") as outfile:
        for fname in file_names:
            with open(fname) as infile:
                for line in infile:
                    outfile.write(line)

    tles = open("tles.txt", "r").readlines()

    print "retrieving TLE file.........."
    # strip off the header tokens and newlines
    tles = [item.strip() for item in tles]

    # clean up the lines
    tles = [(tles[i], tles[i + 1], tles[i + 2]) for i in xrange(0, len(tles) - 2, 3)]
    return tles


# In[6]:


# get_tles()


# In[23]:


def getVectorFile(attributes, input_points, poly_or_line, ogr_output, ogr_format):

    # example usage: getVectorFile(dictionary,list of dicts with lat2 and lon2, 'polygon', SWATH_FILENAME, 'GeoJSON')
    spatialReference = osgeo.osr.SpatialReference()
    spatialReference.ImportFromProj4("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs")
    # if no points passed for ogr build return
    if len(input_points) == 0:
        return ()
    try:
        os.remove(ogr_output)
    except OSError:
        pass
    ogr.UseExceptions()

    driver = ogr.GetDriverByName(ogr_format)

    if os.path.exists(ogr_output):
        driver.DeleteDataSource(ogr_output)
    ds = driver.CreateDataSource(ogr_output)

    if poly_or_line is "polygon":
        geomtype = ogr.wkbPolygon
    if poly_or_line is "line":
        geomtype = ogr.wkbLineString
    if poly_or_line is "point":
        geomtype = ogr.wkbPoint

    if ds is None:
        print "Could not create file"
        sys.exit(1)
    layer = ds.CreateLayer(attributes["Satellite name"], geom_type=geomtype)

    # create a field for the county name
    fieldDefn = ogr.FieldDefn("Satellite Name               :", ogr.OFTString)
    fieldDefn.SetWidth(30)
    layer.CreateField(fieldDefn)
    fieldDefn = ogr.FieldDefn("Orbit height                 :", ogr.OFTString)
    fieldDefn.SetWidth(30)
    layer.CreateField(fieldDefn)
    layer.CreateField(ogr.FieldDefn("Orbit number                 :", ogr.OFTInteger))
    fieldDefn = ogr.FieldDefn("Current UTC time             :", ogr.OFTString)
    fieldDefn.SetWidth(30)
    layer.CreateField(fieldDefn)
    fieldDefn = ogr.FieldDefn("Minutes to horizon           :", ogr.OFTString)
    fieldDefn.SetWidth(30)
    layer.CreateField(fieldDefn)
    fieldDefn = ogr.FieldDefn("Acquisition of Signal UTC    :", ogr.OFTString)
    fieldDefn.SetWidth(30)
    layer.CreateField(fieldDefn)
    fieldDefn = ogr.FieldDefn("Loss of Signal UTC           :", ogr.OFTString)
    fieldDefn.SetWidth(30)
    layer.CreateField(fieldDefn)
    fieldDefn = ogr.FieldDefn("Transit time                 :", ogr.OFTString)
    fieldDefn.SetWidth(30)
    layer.CreateField(fieldDefn)
    fieldDefn = ogr.FieldDefn("Node                         :", ogr.OFTString)
    fieldDefn.SetWidth(30)
    layer.CreateField(fieldDefn)

    featureDefn = layer.GetLayerDefn()

    feature = ogr.Feature(featureDefn)

    feature.SetField("Satellite Name               :", attributes["Satellite name"])
    feature.SetField("Orbit height                 :", attributes["Orbit height"])
    feature.SetField("Orbit number                 :", attributes["Orbit"])
    feature.SetField("Current UTC time             :", str(attributes["Current time"]))
    feature.SetField("Minutes to horizon           :", attributes["Minutes to horizon"])
    feature.SetField("Acquisition of Signal UTC    :", str(attributes["AOS time"]))
    feature.SetField("Loss of Signal UTC           :", str(attributes["LOS time"]))
    feature.SetField("Transit time                 :", str(attributes["Transit time"]))
    feature.SetField("Node                         :", attributes["Node"])

    if poly_or_line == "point":
        point = ogr.Geometry(ogr.wkbPoint)
        for x in input_points:
            point.AddPoint(x["lon2"], x["lat2"], x["alt2"])

        feature.SetGeometry(point)
        layer.CreateFeature(feature)

        point.Destroy()
    if poly_or_line == "line":
        line = ogr.Geometry(type=ogr.wkbLineString)
        for x in input_points:
            line.AddPoint(x["lon2"], x["lat2"], x["alt2"])
            # print x
        feature.SetGeometry(line)
        layer.CreateFeature(feature)

        line.Destroy()

    if poly_or_line == "polygon":
        ring = ogr.Geometry(ogr.wkbLinearRing)

        for x in input_points:
            ring.AddPoint(x["lon2"], x["lat2"])

        poly = ogr.Geometry(ogr.wkbPolygon)
        poly.AddGeometry(ring)

        feature.SetGeometry(poly)

        layer.CreateFeature(feature)

        ring.Destroy()
        poly.Destroy()

    feature.Destroy()

    ds.Destroy()
    # Add altitude to GeoJSON if ogr_format=="GeoJSON" and change colour of track to yellow
    if ogr_format == "GeoJSON":
        if poly_or_line is "line":
            replace_string_in_file(
                ogr_output,
                "<LineString>",
                "<LineString><altitudeMode>absolute</altitudeMode>",
            )
            replace_string_in_file(ogr_output, "ff0000ff", "ffffffff")
        if poly_or_line is "point":
            replace_string_in_file(
                ogr_output, "<Point>", "<Point><altitudeMode>absolute</altitudeMode>"
            )
        if poly_or_line is "polygon":
            replace_string_in_file(
                ogr_output,
                "<PolyStyle><fill>0</fill>",
                "<PolyStyle><color>7f0000ff</color><fill>1</fill>",
            )

    return ()


# In[24]:


def replace_string_in_file(infile, text_to_find, text_to_insert):

    in_file = open(infile, "r")
    temporary = open(os.path.join(output_path, "tmp.txt"), "w")
    for line in in_file:
        temporary.write(line.replace(text_to_find, text_to_insert))
    in_file.close()
    temporary.close()
    os.remove(infile)
    shutil.move(os.path.join(output_path, "tmp.txt"), infile)
    return ()


# In[25]:


def getEffectiveHeading(
    satellite, oi_deg, latitude, longitude, tle_orbit_radius, daily_revolutions
):

    lat_rad = math.radians(latitude)  # Latitude in radians
    oi_rad = math.radians(oi_deg)  # Orbital Inclination (OI) [radians]
    orbit_radius = tle_orbit_radius * 1000.0  # Orbit Radius (R) [m]
    # np = 5925.816                   # Nodal Period [sec] = 5925.816
    np = (24 * 60 * 60) / daily_revolutions
    av = (
        2 * math.pi / np
    )  # Angular Velocity (V0) [rad/sec] =	 0.001060307189285 =2*PI()/E8
    sr = 0  # Sensor Roll (r) [degrees] =	0

    # TODO put earth parameters into a dict and add support for other spheroids GRS1980 etc.
    # Earth Stuff (WGS84)
    one_on_f = 298.257_223_563  # Inverse flattening 1/f = 298.257223563
    f = 1 / one_on_f  # flattening
    r = 6_378_137  # Radius (a) [m] =	 6378137
    e = 1 - math.pow(
        (1 - 1 / one_on_f), 2
    )  # Eccentricity (e^2) = 0.00669438 =1-(1-1/I5)^2
    wO = 0.000_072_722_052  # rotation (w0) [rad/sec] = 7.2722052E-05

    xfac = math.sqrt(1 - e * (2 - e) * (math.pow(math.sin(math.radians(latitude)), 2)))
    phi_rad = math.asin(
        (1 - e) * math.sin(math.radians(latitude)) / xfac
    )  # Phi0' (Geocentric latitude)
    # phi_deg = math.degrees(phi_rad)  # Phi0' (Degrees)
    n = r / math.sqrt(1 - e * (math.pow(math.sin(math.radians(latitude)), 2)))  # N
    altphi_rad = (
        latitude
        - 180
        * math.asin(n * e * math.sin(lat_rad) * math.cos(lat_rad) / orbit_radius)
        / math.pi
    )  # Alt Phi0'(Radians)
    rho_rad = math.acos(
        math.sin(altphi_rad * math.pi / 180) / math.sin(oi_rad)
    )  # Rho (Radians)
    beta = -1 * (
        math.atan(1 / (math.tan(oi_rad) * math.sin(rho_rad))) * 180 / math.pi
    )  # Heading Beta (degrees)
    xn = n * xfac  #  Xn
    altitude = (orbit_radius - xn) / 1000  # altitude
    altitude_ = (
        orbit_radius * math.cos(altphi_rad / 180 * math.pi) / math.cos(lat_rad) - n
    ) / 1000
    rotation = (
        math.atan(
            (wO * math.cos(phi_rad) * math.cos(beta * math.pi / 180))
            / (av + wO * math.cos(phi_rad) * math.sin(beta * math.pi / 180))
        )
        * 180
        / math.pi
    )
    eh = beta + rotation
    alpha12 = eh
    s = 0.5 * 185_000  # s = distance in metres
    effective_heading = alpha12
    return effective_heading


# In[64]:


def addtomap(swathfile, layername):
    if not os.path.isfile(swathfile):
        return ()
    folium.GeoJson(open(swathfile), name=layername).add_to(satellite_map)


def getUpcomingPasses(
    satellite_name, tle_information, passes_begin_time, passes_period
):

    kml = simplekml.Kml()
    observer = ephem.Observer()
    observer.lat = ground_station[0]
    observer.long = ground_station[1]
    observer.horizon = "5:0"
    period = passes_period
    # Get most recent TLE for determining upcoming passes from now
    tles = tle_information

    # make a list of dicts to hold the upcoming pass information for the selected satellites
    schedule = []
    observer.date = passes_begin_time

    while 1:

        print "---------------------------------------"
        for tle in tles:

            if tle[0] == satellite_name:
                # print tle
                # TODO clean up the use of pyephem versus orbital. Orbital can give a orbit number and does many of the pyephem functions
                # TODO add the individual acquisitions as layers in the same ogr output
                # TODO use an appropriate google earth icon for satellites at a visible display resolution with a name tag and minutesaway
                # TODO print output to logging
                satname = str(tle[0]).replace(" ", "_")
                # Flock has minus in filename but looks like GeoJSON creater doesn't like it
                satname = satname.replace("-", "_")
                # print satname

                sat = ephem.readtle(tle[0], tle[1], tle[2])

                twole = tlefile.read(tle[0], "tles.txt")
                now = datetime.utcnow()
                # TODO check age of TLE - if older than x days get_tle()
                print "TLE EPOCH:", twole.epoch
                print "---------------------------------------"
                print tle[0]

                oi = float(str.split(tle[2], " ")[3])
                # orb = Orbital(tle[0])
                orb = Orbital(tle[0], "tles.txt", tle[1], tle[2])
                attributes = []

                rt, ra, tt, ta, st, sa = observer.next_pass(sat)

                # Determine is pass descending or ascending
                # Confirm that observer details have been computed i.e. are not 'Null'

                if rt is None:
                    return ()
                sat.compute(rt)
                aos_lat = sat.sublat.real * (180 / math.pi)

                sat.compute(st)
                los_lat = sat.sublat.real * (180 / math.pi)

                if aos_lat > los_lat:
                    print "PASS                 = descending"
                    node = "descending"
                else:
                    print "PASS                 = ascending"
                    node = "ascending"
                    oi = 360 - oi

                AOStime = datetime.strptime(str(rt), "%Y/%m/%d %H:%M:%S")
                minutesaway = ((AOStime - now).seconds / 60.0) + (
                    (AOStime - now).days * 1440.0
                )

                print "Minutes to horizon   = ", minutesaway
                print "AOStime              = ", rt
                print "LOStime              = ", st
                print "Transit time         = ", tt

                orad = orb.get_lonlatalt(
                    datetime.strptime(str(rt), "%Y/%m/%d %H:%M:%S")
                )[2]

                # Create swath footprint ogr output
                SWATH_FILENAME = os.path.join(
                    output_path,
                    satname
                    + "."
                    + str(
                        orb.get_orbit_number(
                            datetime.strptime(str(rt), "%Y/%m/%d %H:%M:%S")
                        )
                    )
                    + ".ALICE.orbit_swath.geojson",
                )
                attributes = {
                    "Satellite name": satname,
                    "Orbit height": orad,
                    "Orbit": orb.get_orbit_number(
                        datetime.strptime(str(rt), "%Y/%m/%d %H:%M:%S")
                    ),
                    "Current time": str(now),
                    "Minutes to horizon": minutesaway,
                    "AOS time": str(rt),
                    "LOS time": str(st),
                    "Transit time": str(tt),
                    "Node": node,
                    "SWATH_FILENAME": (
                        satname
                        + "."
                        + str(
                            orb.get_orbit_number(
                                datetime.strptime(str(rt), "%Y/%m/%d %H:%M:%S")
                            )
                        )
                        + ".ALICE.orbit_swath.geojson"
                    ),
                    "ORBIT_FILENAME": (
                        satname
                        + "."
                        + str(
                            orb.get_orbit_number(
                                datetime.strptime(str(rt), "%Y/%m/%d %H:%M:%S")
                            )
                        )
                        + ".ALICE.orbit_track.geojson"
                    ),
                }

                # Append the attributes to the list of acquisitions for the acquisition period
                if not any(
                    (
                        x["Satellite name"] == satname
                        and x["Orbit"]
                        == orb.get_orbit_number(
                            datetime.strptime(str(rt), "%Y/%m/%d %H:%M:%S")
                        )
                    )
                    for x in schedule
                ):
                    schedule.append(attributes)

                # Step from AOS to LOS in 100 second intervals
                delta = timedelta(seconds=100)
                deltatime = datetime.strptime(str(rt), "%Y/%m/%d %H:%M:%S")

                geoeastpoint = []
                geowestpoint = []
                geotrack = []

                # print "DELTATIME", deltatime
                # print "SETTING TIME", datetime.strptime(str(st), "%Y/%m/%d %H:%M:%S")

                while deltatime < datetime.strptime(str(st), "%Y/%m/%d %H:%M:%S"):
                    # print "delta time is less than satellite LOS time"
                    sat.compute(deltatime)

                    geotrack.append(
                        {
                            "lat2": sat.sublat.real * (180 / math.pi),
                            "lon2": sat.sublong.real * (180 / math.pi),
                            "alt2": orb.get_lonlatalt(
                                datetime.strptime(str(rt), "%Y/%m/%d %H:%M:%S")
                            )[2]
                            * 1000,
                        }
                    )

                    eastaz = (
                        getEffectiveHeading(
                            sat,
                            oi,
                            sat.sublat.real * (180 / math.pi),
                            sat.sublong.real * (180 / math.pi),
                            orad,
                            sat._n,
                        )
                        + 90
                    )
                    westaz = (
                        getEffectiveHeading(
                            sat,
                            oi,
                            sat.sublat.real * (180 / math.pi),
                            sat.sublong.real * (180 / math.pi),
                            orad,
                            sat._n,
                        )
                        + 270
                    )

                    # Set ground swath per satellite sensor
                    # TODO use view angle check to refine step from satellite track see IFOV
                    if tle[0] in ("LANDSAT 8", "LANDSAT 7"):
                        swath = 185_000 / 2
                    if tle[0] in ("TERRA", "AQUA"):
                        swath = 2_330_000 / 2
                    if tle[0] in ("NOAA 15", "NOAA 18", "NOAA 19"):
                        swath = 2_399_000 / 2
                    if tle[0] == "SUOMI NPP":
                        swath = 2_200_000 / 2
                    if tle[0] == "SENTINEL-2A":
                        swath = 290_000 / 2
                    if tle[0] == "SENTINEL-2B":
                        swath = 290_000 / 2

                    geoeastpoint.append(
                        Geodesic.WGS84.Direct(
                            sat.sublat.real * 180 / math.pi,
                            sat.sublong.real * 180 / math.pi,
                            eastaz,
                            swath,
                        )
                    )
                    geowestpoint.append(
                        Geodesic.WGS84.Direct(
                            sat.sublat.real * 180 / math.pi,
                            sat.sublong.real * 180 / math.pi,
                            westaz,
                            swath,
                        )
                    )

                    deltatime = deltatime + delta

                # Create current location ogr output

                nowpoint = [
                    {
                        "lat2": orb.get_lonlatalt(datetime.utcnow())[1],
                        "lon2": orb.get_lonlatalt(datetime.utcnow())[0],
                        "alt2": orb.get_lonlatalt(datetime.utcnow())[2] * 1000,
                    }
                ]

                # TODO ensure the now attributes are actually attributes for the current position of the satellite and include relevant next pass information...tricky?
                # if ((attributes['Orbit']==orb.get_orbit_number(datetime.utcnow()))and(AOStime<now)):
                now_attributes = {
                    "Satellite name": satname,
                    "Orbit height": orb.get_lonlatalt(datetime.utcnow())[2],
                    "Orbit": orb.get_orbit_number(datetime.utcnow()),
                    "Current time": str(now),
                    "Minutes to horizon": "N/A",
                    "AOS time": "N/A",
                    "LOS time": "N/A",
                    "Transit time": "N/A",
                    "Node": "N/A",
                }
                # now_attributes=attributes

                CURRENT_POSITION_FILENAME = os.path.join(
                    output_path, satname + "_current_position.geojson"
                )

                # TODO draw the current orbit forward for the passes period time from the satellite position as a long stepped ogr line
                # print now_attributes,nowpoint
                getVectorFile(
                    now_attributes,
                    nowpoint,
                    "point",
                    CURRENT_POSITION_FILENAME,
                    "GeoJSON",
                )

                polypoints = []

                for x in geowestpoint:
                    polypoints.append({"lat2": x["lat2"], "lon2": x["lon2"]})
                for x in reversed(geoeastpoint):
                    polypoints.append({"lat2": x["lat2"], "lon2": x["lon2"]})
                if len(polypoints) > 0:
                    polypoints.append(
                        {
                            "lat2": geowestpoint[0]["lat2"],
                            "lon2": geowestpoint[0]["lon2"],
                        }
                    )

                # Create swath footprint ogr output
                SWATH_FILENAME = os.path.join(
                    output_path,
                    satname
                    + "."
                    + str(
                        orb.get_orbit_number(
                            datetime.strptime(str(rt), "%Y/%m/%d %H:%M:%S")
                        )
                    )
                    + ".ALICE.orbit_swath.geojson",
                )
                ORBIT_FILENAME = os.path.join(
                    output_path,
                    satname
                    + "."
                    + str(
                        orb.get_orbit_number(
                            datetime.strptime(str(rt), "%Y/%m/%d %H:%M:%S")
                        )
                    )
                    + ".ALICE.orbit_track.geojson",
                )
                TRACKING_SWATH_FILENAME = os.path.join(
                    output_path, satname + "_tracking_now.geojson"
                )

                # Create currently acquiring polygon
                # TODO def this
                # Step from AOS to current time second intervals

                observer.date = now
                sat.compute(observer)
                tkdelta = timedelta(seconds=100)
                # TODO Problem determining rise time if rise time has passed and set time not yet reached
                # solution may be to replace rt with now if rt > st
                tkrt, tkra, tktt, tkta, tkst, tksa = observer.next_pass(sat)
                # print tkrt, tkra, tktt, tkta, tkst, tksa
                if tkrt is None:
                    return ()
                    # if datetime.strptime(str(tkrt),"%Y/%m/%d %H:%M:%S") > datetime.strptime(str(tkst),"%Y/%m/%d %H:%M:%S"):
                    #    tkrt = datetime.strptime(str(tkst),"%Y/%m/%d %H:%M:%S") - datetime.strptime(str(tktt),"%Y/%m/%d %H:%M:%S")
                    #    tkrt = str(tkt),"%Y/%m/%d %H:%M:%S"
                    # print "NOW: ",now," TKRT: ",datetime.strptime(str(tkrt),"%Y/%m/%d %H:%M:%S")," TKST: ",datetime.strptime(str(tkst),"%Y/%m/%d %H:%M:%S")
                tkdeltatime = datetime.utcnow()
                tkgeoeastpoint = []
                tkgeowestpoint = []
                tkgeotrack = []

                # while tkdeltatime < (datetime.utcnow() or datetime.strptime(str(tkst),"%Y/%m/%d %H:%M:%S")):
                while tkdeltatime < datetime.strptime(str(tkst), "%Y/%m/%d %H:%M:%S"):

                    sat.compute(tkdeltatime)
                    tkgeotrack.append(
                        {
                            "lat2": sat.sublat.real * (180 / math.pi),
                            "lon2": sat.sublong.real * (180 / math.pi),
                            "alt2": orb.get_lonlatalt(
                                datetime.strptime(str(rt), "%Y/%m/%d %H:%M:%S")
                            )[2],
                        }
                    )

                    tkeastaz = (
                        getEffectiveHeading(
                            sat,
                            oi,
                            sat.sublat.real * (180 / math.pi),
                            sat.sublong.real * (180 / math.pi),
                            orad,
                            sat._n,
                        )
                        + 90
                    )
                    tkwestaz = (
                        getEffectiveHeading(
                            sat,
                            oi,
                            sat.sublat.real * (180 / math.pi),
                            sat.sublong.real * (180 / math.pi),
                            orad,
                            sat._n,
                        )
                        + 270
                    )
                    # TODO use view angle check to refine step from satellite track see IFOV
                    if tle[0] in ("LANDSAT 8", "LANDSAT 7"):
                        tkswath = 185_000 / 2
                    if tle[0] in ("TERRA", "AQUA"):
                        tkswath = 2_330_000 / 2
                    if tle[0] in ("NOAA 15", "NOAA 18", "NOAA 19"):
                        tkswath = 1_100_000 / 2
                    if tle[0] == "SUOMI NPP":
                        tkswath = 2_200_000 / 2
                    if tle[0] == "SENTINEL-2A":
                        tkswath = 290_000 / 2
                    if tle[0] == "SENTINEL-2B":
                        tkswath = 290_000 / 2

                    tkgeoeastpoint.append(
                        Geodesic.WGS84.Direct(
                            sat.sublat.real * 180 / math.pi,
                            sat.sublong.real * 180 / math.pi,
                            tkeastaz,
                            tkswath,
                        )
                    )
                    tkgeowestpoint.append(
                        Geodesic.WGS84.Direct(
                            sat.sublat.real * 180 / math.pi,
                            sat.sublong.real * 180 / math.pi,
                            tkwestaz,
                            tkswath,
                        )
                    )

                    tkdeltatime = tkdeltatime + tkdelta

                tkpolypoints = []

                for x in tkgeowestpoint:
                    tkpolypoints.append({"lat2": x["lat2"], "lon2": x["lon2"]})
                for x in reversed(tkgeoeastpoint):
                    tkpolypoints.append({"lat2": x["lat2"], "lon2": x["lon2"]})
                if len(tkpolypoints) > 0:
                    tkpolypoints.append(
                        {
                            "lat2": tkgeowestpoint[0]["lat2"],
                            "lon2": tkgeowestpoint[0]["lon2"],
                        }
                    )

                    # if not ((attributes['Node']=="ascending")and(satname not in ("AQUA"))):
                    if (
                        (attributes["Node"] == "ascending")
                        and (satname in ("AQUA", "SUOMI_NPP"))
                    ) or (
                        (attributes["Node"] == "descending")
                        and (satname not in ("AQUA", "SUOMI_NPP"))
                    ):
                        # Create swath ogr output
                        getVectorFile(
                            attributes, polypoints, "polygon", SWATH_FILENAME, "GeoJSON"
                        )
                        # Create orbit track ogr output
                        getVectorFile(
                            attributes, geotrack, "line", ORBIT_FILENAME, "GeoJSON"
                        )
                        addtomap(
                            SWATH_FILENAME,
                            satname
                            + "."
                            + str(
                                orb.get_orbit_number(
                                    datetime.strptime(str(rt), "%Y/%m/%d %H:%M:%S")
                                )
                            )
                            + ".swath",
                        )
                        addtomap(
                            ORBIT_FILENAME,
                            satname
                            + "."
                            + str(
                                orb.get_orbit_number(
                                    datetime.strptime(str(rt), "%Y/%m/%d %H:%M:%S")
                                )
                            )
                            + ".orbit",
                        )
                        addtomap(CURRENT_POSITION_FILENAME, satname)

                        # Create currently acquiring ogr output
                        # print "NOW: ",now," TKRT: ",datetime.strptime(str(tkrt),"%Y/%m/%d %H:%M:%S")," TKST: ",datetime.strptime(str(tkst),"%Y/%m/%d %H:%M:%S")
                        # if ((datetime.strptime(str(tkrt),"%Y/%m/%d %H:%M:%S")>now) and (datetime.strptime(str(tkst),"%Y/%m/%d %H:%M:%S")<now)):
                        if tkrt > tkst:
                            print "Executing tracking swath creation - tkpolypoints = ", tkpolypoints
                            getVectorFile(
                                now_attributes,
                                tkpolypoints,
                                "polygon",
                                TRACKING_SWATH_FILENAME,
                                "GeoJSON",
                            )

                baseline = 1
                if minutesaway <= period:

                    print "---------------------------------------"
                    print tle[0], "WILL BE MAKING A PASS IN ", minutesaway, " MINUTES"
                    print " Rise Azimuth: ", ra
                    print " Transit Time: ", tt
                    print " Transit Altitude: ", ta
                    print " Set Time: ", st
                    print " Set Azimuth: ", sa
                    # Create a temporal KML
                    print (
                        satname
                        + "_"
                        + str(
                            orb.get_orbit_number(
                                datetime.strptime(str(rt), "%Y/%m/%d %H:%M:%S")
                            )
                        )
                    )
                    pol = kml.newpolygon(
                        name=satname
                        + "_"
                        + str(
                            orb.get_orbit_number(
                                datetime.strptime(str(rt), "%Y/%m/%d %H:%M:%S")
                            )
                        ),
                        description=SWATH_FILENAME,
                    )
                    kml_polypoints = []
                    for i in polypoints:
                        kml_polypoints.append((i["lon2"], i["lat2"]))
                    rt_kml = datetime.strptime(str(rt), "%Y/%m/%d %H:%M:%S")
                    st_kml = datetime.strptime(str(st), "%Y/%m/%d %H:%M:%S")
                    pol.outerboundaryis = kml_polypoints
                    pol.style.linestyle.color = simplekml.Color.green

                    pol.style.linestyle.width = 5
                    pol.style.polystyle.color = simplekml.Color.changealphaint(
                        100, simplekml.Color.green
                    )
                    pol.timespan.begin = rt_kml.strftime("%Y-%m-%dT%H:%M:%SZ")
                    pol.timespan.end = st_kml.strftime("%Y-%m-%dT%H:%M:%SZ")

                    for x in sorted(schedule, key=lambda k: k["AOS time"]):
                        # print x

                        # For dictionary entries with 'LOS time' older than now time - remove
                        if (
                            datetime.strptime(str(x["LOS time"]), "%Y/%m/%d %H:%M:%S")
                        ) < (datetime.utcnow()):
                            # Delete output ogr
                            if os.path.exists(
                                os.path.join(
                                    output_path,
                                    satname
                                    + "."
                                    + str(x["Orbit"])
                                    + ".ALICE.orbit_swath.geojson",
                                )
                            ):
                                shutil.move(
                                    os.path.join(
                                        output_path,
                                        satname
                                        + "."
                                        + str(x["Orbit"])
                                        + ".ALICE.orbit_swath.geojson",
                                    ),
                                    os.path.join(
                                        output_path,
                                        satname
                                        + "."
                                        + str(x["Orbit"])
                                        + ".ALICE.orbit_swath.geojson.OUTOFDATE",
                                    ),
                                )
                            if os.path.exists(
                                os.path.join(
                                    output_path,
                                    satname
                                    + "."
                                    + str(x["Orbit"])
                                    + ".ALICE.orbit_track.geojson",
                                )
                            ):
                                shutil.move(
                                    os.path.join(
                                        output_path,
                                        satname
                                        + "."
                                        + str(x["Orbit"])
                                        + ".ALICE.orbit_track.geojson",
                                    ),
                                    os.path.join(
                                        output_path,
                                        satname
                                        + "."
                                        + str(x["Orbit"])
                                        + ".ALICE.orbit_track.geojson.OUTOFDATE",
                                    ),
                                )

                            # Delete dictionary entry for pass
                            schedule.remove(x)

                    # Unlikely - if no entries in the schedule don't try to print it
                    # see if there are any new additions to the schedule

                    if len(schedule) > 0:
                        print (
                            datetime.strptime(
                                str(schedule[0]["AOS time"]), "%Y/%m/%d %H:%M:%S"
                            )
                        )

                    # If the AOS time is less than now + the time delta, shift the time to the latest recorded pass LOS time
                    if len(schedule) > 0 and (
                        (
                            datetime.strptime(
                                str(schedule[len(schedule) - 1]["AOS time"]),
                                "%Y/%m/%d %H:%M:%S",
                            )
                            < (now + timedelta(minutes=period))
                        )
                    ):
                        observer.date = datetime.strptime(
                            str(schedule[len(schedule) - 1]["LOS time"]),
                            "%Y/%m/%d %H:%M:%S",
                        ) + timedelta(minutes=5)

                        # Recompute the satellite position for the update time
                        sat.compute(observer)
                        # print "MODIFIED OBSERVER DATE",observer.date

                    else:
                        print "--------NOTHING TO MODIFY MOVING TO NEXT SATELLITE IN LIST------"
                        kml.save(os.path.join(output_path, satname + ".ALICE.kml"))
                        # TODO - write to html
                        # Exit the def if the schedule isn't able to update because there are no passes in the acquisition window

                        html_output = open(
                            os.path.join(output_path, satname + ".schedule.html"), "w"
                        )
                        html_output.write("<!DOCTYPE html>" + "\n")
                        html_output.write("<html>" + "\n")
                        html_output.write("<head>" + "\n")
                        html_output.write(
                            "    <title>Satellite Earth Footprints</title>" + "\n"
                        )
                        html_output.write(
                            '	<meta http-equiv="refresh" content="20">' + "\n"
                        )
                        # html_output.write('	<meta charset="utf-8" />'+'\n')
                        html_output.write(
                            '    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">'
                            + "\n"
                        )
                        # html_output.write('	<meta name="viewport" content="width=device-width, initial-scale=1.0">'+'\n')
                        html_output.write(
                            '    <link rel="stylesheet" href="../leaflet.css" />' + "\n"
                        )
                        html_output.write(
                            '    <link rel="stylesheet" href="../src/L.Control.Sidebar.css" />'
                            + "\n"
                        )
                        html_output.write("    <style>" + "\n")
                        html_output.write("        body {" + "\n")
                        html_output.write("            padding: 0;" + "\n")
                        html_output.write("            margin: 0;" + "\n")
                        html_output.write("        }" + "\n")

                        html_output.write("        html, body, #map {" + "\n")
                        html_output.write("            height: 100%;" + "\n")
                        html_output.write("        }" + "\n")

                        html_output.write("        .lorem {" + "\n")
                        html_output.write("            font-style: italic;" + "\n")
                        html_output.write("            color: #AAA;" + "\n")
                        html_output.write("        }" + "\n")
                        html_output.write("	</style>" + "\n")

                        html_output.write("</head>" + "\n")
                        html_output.write("<body>" + "\n")
                        html_output.write('    <div id="sidebar">' + "\n")
                        html_output.write("		<h1>Observation footprints</h1>" + "\n")

                        html_output.write(
                            '		<h5 id="demo">"Time of last refresh"</h5>' + "\n"
                        )
                        html_output.write(
                            '		<script> var d = new Date();document.getElementById("demo").innerHTML = d.toUTCString();</script>'
                            + "\n"
                        )

                        html_output.write("		<ul>" + "\n")
                        for y in sorted(satellites):
                            y = y.replace(" ", "_").replace("-", "_")

                            html_output.write(
                                '            <li><a href="'
                                + y
                                + ".schedule.html"
                                + '">'
                                + y
                                + " schedule"
                                + "</a></li>"
                                + "\n"
                            )
                        html_output.write("		</ul>" + "\n")

                        html_output.write("		<table>" + "\n")

                        html_output.write("		<tr>" + "\n")
                        html_output.write("			<th>Satellite</th>" + "\n")
                        html_output.write("			<th>Orbit</th>" + "\n")
                        html_output.write("			<th>Node</th>" + "\n")
                        html_output.write("			<th>AOS time</th>" + "\n")
                        html_output.write("			<th>LOS time</th>" + "\n")
                        html_output.write("			<th>Minutes to horizon</th>" + "\n")
                        html_output.write("		</tr>" + "\n")

                        for x in sorted(schedule):
                            if (
                                (
                                    (x["Node"] == "ascending")
                                    and (x["Satellite name"] in ("AQUA", "SUOMI_NPP"))
                                )
                                or (
                                    (x["Node"] == "descending")
                                    and (
                                        x["Satellite name"] not in ("AQUA", "SUOMI_NPP")
                                    )
                                )
                                and (int(x["Minutes to horizon"]) <= period)
                            ):
                                html_output.write("		<tr>" + "\n")
                                html_output.write(
                                    "			<td>"
                                    + str(x["Satellite name"])
                                    + "</td>"
                                    + "\n"
                                )
                                html_output.write(
                                    '			<td><a href="'
                                    + str(x["SWATH_FILENAME"])
                                    + '">'
                                    + str(x["Orbit"])
                                    + "</a></td>"
                                    + "\n"
                                )
                                html_output.write(
                                    '			<td><a href="'
                                    + str(x["ORBIT_FILENAME"])
                                    + '">'
                                    + str(x["Node"])
                                    + "</a></td>"
                                    + "\n"
                                )
                                html_output.write(
                                    "			<td>" + str(x["AOS time"]) + "</td>" + "\n"
                                )
                                html_output.write(
                                    "			<td>" + str(x["LOS time"]) + "</td>" + "\n"
                                )
                                html_output.write(
                                    '			<td><a href="'
                                    + satname
                                    + "_current_position.geojson"
                                    + '">'
                                    + str(x["Minutes to horizon"])
                                    + "</a></td>"
                                    + "\n"
                                )
                                html_output.write("		</tr>" + "\n")
                        html_output.write("		</table>" + "\n")
                        html_output.write("    </div>" + "\n")

                        html_output.write('    <div id="map"></div>' + "\n")

                        html_output.write(
                            '	 <script src="../leaflet.js"></script>' + "\n"
                        )
                        # html_output.write('	 <script src="../jquery-1.11.3.min.js"></script>'+'\n')
                        html_output.write(
                            '	 <script src="../src/L.Control.Sidebar.js"></script>'
                            + "\n"
                        )

                        html_output.write("	 <script>" + "\n")

                        html_output.write(
                            "		var map = L.map('map').setView([-26.0, 132.0], 3);"
                            + "\n"
                        )

                        html_output.write(
                            "		L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token=pk.eyJ1IjoibWFwYm94IiwiYSI6IjZjNmRjNzk3ZmE2MTcwOTEwMGY0MzU3YjUzOWFmNWZhIn0.Y8bhBaUMqFiPrDRW9hieoQ', {"
                            + "\n"
                        )
                        html_output.write("			maxZoom: 18," + "\n")
                        html_output.write(
                            "			attribution: 'Map data &copy; <a href="
                            + '"http://openstreetmap.org">OpenStreetMap</a> contributors, '
                            "' +" + "\n"
                        )
                        html_output.write(
                            "				'<a href="
                            + '"http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, '
                            "' +" + "\n"
                        )
                        html_output.write(
                            "				'Imagery <a href="
                            + '"http://mapbox.com">Mapbox</a>'
                            + "',"
                            + "\n"
                        )
                        html_output.write("			id: 'mapbox.streets'" + "\n")
                        html_output.write("		}).addTo(map);" + "\n")

                        driver = ogr.GetDriverByName("GeoJSON")
                        for x in sorted(schedule):
                            if (
                                (
                                    (x["Node"] == "ascending")
                                    and (x["Satellite name"] in ("AQUA", "SUOMI_NPP"))
                                )
                                or (
                                    (x["Node"] == "descending")
                                    and (
                                        x["Satellite name"] not in ("AQUA", "SUOMI_NPP")
                                    )
                                )
                                and (int(x["Minutes to horizon"]) <= period)
                            ):
                                ogrswath = os.path.join(
                                    output_path,
                                    str(x["Satellite name"])
                                    + "."
                                    + str(x["Orbit"])
                                    + ".ALICE.orbit_swath.geojson",
                                )
                                ogrtrack = os.path.join(
                                    output_path,
                                    str(x["Satellite name"])
                                    + "."
                                    + str(x["Orbit"])
                                    + ".ALICE.orbit_track.geojson",
                                )
                                ogrposition = os.path.join(
                                    output_path,
                                    str(x["Satellite name"])
                                    + "_current_position.geojson",
                                )

                                dataSource = driver.Open(ogrswath, 0)
                                layer = dataSource.GetLayer()

                                # Add the polygon features
                                for feature in layer:
                                    geom = feature.GetGeometryRef()

                                    html_output.write("		L.polygon([" + "\n")

                                    geomlist = (
                                        str(geom)
                                        .replace("POLYGON ((", "")
                                        .replace(")", "")
                                        .replace("[", "")
                                        .replace("]", "")
                                        .split(",")
                                    )

                                    for i in geomlist:

                                        xyz = i.split()

                                        html_output.write(
                                            "			[ "
                                            + xyz[1]
                                            + ","
                                            + xyz[0]
                                            + ","
                                            + xyz[2]
                                            + "], "
                                            + "\n"
                                        )

                                    html_output.write(
                                        '		]).addTo(map).bindPopup("<b>Satellite: '
                                        + x["Satellite name"]
                                        + "</b><br /><b>Orbit: "
                                        + str(x["Orbit"])
                                        + "</b><br /><b>Orbit height: "
                                        + str(x["Orbit height"])
                                        + "</b><br /><b>Minutes to horizon: "
                                        + str(x["Minutes to horizon"])
                                        + "</b><br /><b>AOS time: "
                                        + str(x["AOS time"])
                                        + "</b><br /><b>LOS time: "
                                        + str(x["LOS time"])
                                        + '</b>");'
                                        + "\n"
                                    )

                                # Add the line features
                                dataSourceTrack = driver.Open(ogrtrack, 0)
                                layertrack = dataSourceTrack.GetLayer()

                                for feature in layertrack:
                                    geom = feature.GetGeometryRef()

                                    html_output.write("		L.polyline([" + "\n")
                                    geomlist = (
                                        str(geom)
                                        .replace("LINESTRING", "")
                                        .replace("(", "")
                                        .split(",")
                                    )

                                    for i in geomlist:

                                        xyz = i.split()

                                        html_output.write(
                                            "			[ "
                                            + xyz[1]
                                            + ","
                                            + xyz[0]
                                            + "], "
                                            + "\n"
                                        )

                                    html_output.write(
                                        "		],{color:'white'}).addTo(map).bindPopup"
                                        + '("<b>Satellite: '
                                        + x["Satellite name"]
                                        + "</b><br /><b>Orbit: "
                                        + str(x["Orbit"])
                                        + "</b><br /><b>Orbit height: "
                                        + str(x["Orbit height"])
                                        + "</b><br /><b>Minutes to horizon: "
                                        + str(x["Minutes to horizon"])
                                        + "</b><br /><b>AOS time: "
                                        + str(x["AOS time"])
                                        + "</b><br /><b>LOS time: "
                                        + str(x["LOS time"])
                                        + '</b>");'
                                        + "\n"
                                    )

                        # Add current position

                        html_output.write("		var myIcon = L.icon({" + "\n")
                        html_output.write(
                            "		     iconUrl: 'satellite_icon.png'," + "\n"
                        )

                        html_output.write("		     iconSize: [44, 21]," + "\n")

                        html_output.write("		     popupAnchor: [-3, -76]," + "\n")

                        html_output.write("		     });" + "\n")

                        dataSourcePosition = driver.Open(ogrposition, 0)
                        layerposition = dataSourcePosition.GetLayer()
                        for feature in layerposition:
                            geom = feature.GetGeometryRef()

                            xyz = (
                                str(geom).replace("POINT", "").replace("(", "").split()
                            )

                        html_output.write(
                            "		L.marker(["
                            + xyz[1]
                            + ","
                            + xyz[0]
                            + "], {icon: myIcon}).addTo(map).bindPopup"
                            + '("<b>Current Position: '
                            + x["Satellite name"]
                            + '</b>");'
                            + "\n"
                        )

                        html_output.write("		var popup = L.popup();" + "\n")

                        html_output.write(
                            "        var sidebar = L.control.sidebar('sidebar', {"
                            + "\n"
                        )
                        html_output.write("            closeButton: true," + "\n")
                        html_output.write("            position: 'left'" + "\n")
                        html_output.write("        });" + "\n")
                        html_output.write("        map.addControl(sidebar);" + "\n")

                        html_output.write("        setTimeout(function () {" + "\n")
                        html_output.write("            sidebar.show();" + "\n")
                        html_output.write("        }, 500);" + "\n")

                        html_output.write(
                            "        var marker = L.marker([-26, 132]).addTo(map).on('click', function () {"
                            + "\n"
                        )
                        html_output.write("            sidebar.toggle();" + "\n")
                        html_output.write("        });" + "\n")

                        html_output.write(
                            "        map.on('click', function () {" + "\n"
                        )
                        html_output.write("            sidebar.hide();" + "\n")
                        html_output.write("        })" + "\n")

                        html_output.write(
                            "        sidebar.on('show', function () {" + "\n"
                        )
                        html_output.write(
                            "            console.log('Sidebar will be visible.');"
                            + "\n"
                        )
                        html_output.write("        });" + "\n")

                        html_output.write(
                            "        sidebar.on('shown', function () {" + "\n"
                        )
                        html_output.write(
                            "            console.log('Sidebar is visible.');" + "\n"
                        )
                        html_output.write("        });" + "\n")

                        html_output.write(
                            "        sidebar.on('hide', function () {" + "\n"
                        )
                        html_output.write(
                            "            console.log('Sidebar will be hidden.');" + "\n"
                        )
                        html_output.write("        });" + "\n")

                        html_output.write(
                            "        sidebar.on('hidden', function () {" + "\n"
                        )
                        html_output.write(
                            "            console.log('Sidebar is hidden.');" + "\n"
                        )
                        html_output.write("        });" + "\n")

                        html_output.write(
                            "        L.DomEvent.on(sidebar.getCloseButton(), 'click', function () {"
                            + "\n"
                        )
                        html_output.write(
                            "            console.log('Close button clicked.');" + "\n"
                        )
                        html_output.write("        });" + "\n")

                        html_output.write("	</script>" + "\n")

                        html_output.write("</body>" + "\n")
                        html_output.write("</html>" + "\n")

                        return ()

        time.sleep(1 * sleep_status)
    return ()


# In[66]:


### set up folium plot
style_function = lambda x: {
    "fillColor": "#000000" if x["type"] == "Polygon" else "#00ff00"
}
satellite_map = folium.Map(location=[-30, 150], tiles="Mapbox Bright", zoom_start=4)
##

if __name__ == "__main__":

    tles = get_tles()
    tle_retrieve_time = datetime.utcnow()
    # Loop through satellite list and execute until end of period

    # satellites = ("SENTINEL-2A","LANDSAT 7", "LANDSAT 8", "TERRA", "AQUA", "NOAA 15", "NOAA 18", "NOAA 19", "SUOMI NPP")
    satellites = ["SENTINEL-2A", "SENTINEL-2B", "LANDSAT 8", "LANDSAT 7"]
    # while 1:
    for i in satellites:

        print "Looking for ", i
        getUpcomingPasses(i, tles, datetime.utcnow(), period)
        # check for stale tle and update if required
        tle_timesinceretrieve = datetime.now() - tle_retrieve_time
        print "Time since tle retrieved from celestrak: ", tle_timesinceretrieve
        if tle_timesinceretrieve > timedelta(hours=24):
            get_tles()
            tle_retrieve_time = datetime.utcnow()

## plot folium map
folium.LayerControl().add_to(satellite_map)
satellite_map


# In[ ]:
