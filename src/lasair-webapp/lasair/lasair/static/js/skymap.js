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
