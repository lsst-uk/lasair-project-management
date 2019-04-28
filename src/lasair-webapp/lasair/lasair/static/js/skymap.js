var aladin;
var skymap_id = '';
var currentJsonData = {};
var ZTFdata;

var deg = 180/Math.PI;

function setAladinSkymaps(passAladin){
    aladin = passAladin;
}

function setSkymap_id(passSkymap_id){
    skymap_id = passSkymap_id;
}

function haveSkymap(){   // Is there a skymap in the house
    return ('datetime' in currentJsonData);
}

function change_square_size(factor){
    square_size *= factor;
    redrawAll();
}

function handleSkymap(data){
    datam = data['meta']
    if('DATE-OBS' in datam){
        data['datetime'] = datam['DATE-OBS'].slice(0,19);   // forget the seconds
    } else if ('DATE' in datam) {
        data['datetime'] = datam['DATE'].slice(0,19);   // forget the seconds
    } else {
        data['datetime'] = '';
    }

    currentJsonData = data;        // keep this for later refreshes
    console.log("Imported the JSON " + data['datetime']);
}

var square_size = 0.08;
var color = 'yellow';

function makeGalaxies(data){
    console.log('found ' + data.sources.length + ' galaxies');
    var overlay = aladin.createOverlay({color: color});
    aladin.addOverlay(overlay);
    for(var i=0; i<data.sources.length; i++){
        t = data.sources[i];
        square = makeSquare(t.coords[0], t.coords[1], square_size*t.sw);
        overlay.addFootprints([A.polygon(square)]);
    }
}

function makeGalaxyTable(data){
    var s = '200 most probable galaxies <table cellpadding=5>';
    s += '<tr><th>Name<br/>(NED link)</th><th>Percent<br/>probability</th><th>Distance<br/>(Mpc)</th></tr>';
    for(var i=0; i<data.sources.length; i++){
        t = data.sources[i];
        var aw = t.absw*100;
        distance = (t.distance).toString().substring(0,5);
        aw = aw.toString();
        aw = aw.substring(0,4);
        var ra = t.coords[0];
        var de = t.coords[1];
        url = 'http://ned.ipac.caltech.edu/cgi-bin/nph-objsearch?lon='+ra+'d&lat='+de+'d&radius=0.25&search_type=Near+Position+Search';
        s += '<tr>';
        s += '<td><a href="' + url + '">' + t.name + '</a></td>';
        s += '<td>' + aw + '</td>';
        s += '<td>' + distance + '</td>';
        s += '</tr>';
    }
    s += '</table>';
    document.getElementById("galaxies_table").innerHTML = s;
}

function makeSquare(ra, de, r){
    poly = [];
    rr = r/Math.cos(de*Math.PI/180);
    poly.push([ra-rr, de-r]);
    poly.push([ra+rr, de-r]);
    poly.push([ra+rr, de+r]);
    poly.push([ra-rr, de+r]);
   return poly;
}

function handleDblclick(offsetX, offsetY){
    var coo = aladin.pix2world(offsetX,offsetY);
    var p = coo[0];
    var q = coo[1];
    console.log('handleDblClick ' + p + ' ' + q);
    ra_ratio = 1.0/Math.cos(q*Math.PI/180);
    sources = currentJsonData.sources;

    selected_sources = [];
    for(var i=0; i<sources.length; i++){
        t = sources[i];
        var sw = t.sw*square_size;
        var ra = t.coords[0]
        var de = t.coords[1];
        if(Math.abs(ra-p)<sw*ra_ratio && Math.abs(de-q)<sw){
            gotoradec(ra, de);
            var url = 'http://ned.ipac.caltech.edu/cgi-bin/nph-objsearch?lon='+ra+'d&lat='+de+'d&radius=0.25&search_type=Near+Position+Search';
            if(t.distance < 0.0001){
                dist = '';
            } else {
                dist = ' at ' + (t.distance).toFixed(1) + ' Mpc. ';
            }
            document.getElementById("galaxy_info").innerHTML = 
                'You chose galaxy <a href="' + url + '" target="_blank">' + t.name + '</a>' + dist;
        }
    }
}

function moveCenter(){
    gotoradec(currentJsonData.meta.apointRA, currentJsonData.meta.apointDec);
}

function tileMarkers(){
    var cat = A.catalog({name: 'Markers', sourceSize: 10});
    aladin.addCatalog(cat);
    tilelist = currentJsonData.meta.histogram;
    console.log("tilelist length " + tilelist.length);
    for(var i=0; i<tilelist.length; i++){
        cat.addSources([A.marker(2.5+5*tilelist[i][0], 87.5-5*tilelist[i][1])]);
    }
}

function redrawAll(){
    console.log("redrawAll");
    clearall();
    drawLayers(currentJsonData);
}

function makeContours(data){
    if(!('contours' in data)) return;
    for(var i=0; i<data.contours.length; i++){
        var c = data.contours[i];
        poly = "Polygon J2000 ";
        coo = c.coords;
        sv_polyline(coo, '#' + c.color );
    }
}

function gotoradec(ra, dec){
    aladin.gotoPosition(ra, dec);
}

function sv_polygon(points, color){
    var overlay = aladin.createOverlay({color: color});
    aladin.addOverlay(overlay);
    curPolygon = [];
    for(var i=0; i<points.length; i++){
        ra = points[i][0];
        dec = points[i][1];
        curPolygon.push([ra, dec]);
    }
    var x = A.polygon(curPolygon);
    overlay.addFootprints([x]);
}

function sv_polyline(points, color){
    var overlay = aladin.createOverlay({color: color});
    aladin.addOverlay(overlay);
    curPolyline = [];
    for(var i=0; i<points.length; i++){
        ra = points[i][0];
        dec = points[i][1];
        curPolyline.push([ra, dec]);
    }
    var x = A.polyline(curPolyline);
    overlay.add(x);
}

function clearall(){
    aladin.removeLayers();
}

function urlFromSkymap_id(skymap_id){
    return '/lasair/static/ztf/skymap/' + skymap_id + '.json'
}

function setup(passSkymap_id, ztf_json_data) {
    skymap_id = passSkymap_id;

    var fov = 180;   // field of view in degrees
    var aladin = $.aladin('#aladin-lite-div', {survey: "P/Mellinger/color", zoom: fov, showGotoControl: true, showLayersControl:true});
    $('input[name=survey]').change(function() {
        aladin.setImageSurvey($(this).val());
    });

    $('#projectionChoice').change(function() {
        aladin.setProjection($(this).val());
    });

    aladin.setProjection("sinus");
    setAladinSkymaps(aladin);

    jsonurl = urlFromSkymap_id(skymap_id);
    console.log("Fetching skymap info " + jsonurl);
    $.ajax({
        dataType: "json",
        url: jsonurl,
        success: handleSkymap,
        async: false,
        error: function(xhr, ajaxOptions, thrownError) {
            alert("No skymap found for " + skymap_id);}
    });

    redrawAll();
    moveCenter();
    handleZTF(ztf_json_data);
    aladin.showLayerBox();
    aladin.showReticle(false);
}

function handleZTF(ztf_json_data){
    console.log("Found " + ztf_json_data.length + " entries");
    var cat = A.catalog({name: 'ZTF candidates', sourceSize: 10, color:'blue'});
    aladin.addCatalog(cat);
    for(var i=0; i<ztf_json_data.length; i++){
        z = ztf_json_data[i];
        // z.objectId, z.ra, z.dec, z.fid, z.magpsf
        desc = '<a target="_blank" href="https://lasair.roe.ac.uk/object/' + z.objectId + '">' + z.objectId + '</a>';
        cat.addSources([A.marker(z.ra, z.dec, {popupTitle: '', popupDesc: desc})]);
    }
}
