var aladin;
var q = {};
var maxn;

function setupAladin(){
    console.log("Setting up Aladin");
    var fov = 180;   // field of view in degrees
    aladin = $.aladin('#aladin-lite-div', {survey: "P/Mellinger/color", zoom: fov, showGotoControl: false});
    aladin.gotoRaDec(285.0, 30.0)

//    aladin.setProjection("aitoff");    
    aladin.setProjection("sinus");    
}

function queryCoverage(passnid1, passnid2) {
    nid1 = passnid1;
    nid2 = passnid2;

    jsonurl = '/coverageAjax/' + nid1 + '/' + nid2 + '/'
    console.log("Fetching coverage info " + jsonurl);
    $.ajax({
        dataType: "json",
        url: jsonurl,
        success: handleCoverage,
        async: false,
        error: function(xhr, ajaxOptions, thrownError) {
            alert("Error fetching coverage");}
    });
}

function handleCoverage(data){
    if('result' in data) {
        console.log("Found " + data.result.length + " entries")
    }
    q = data;
    maxn = 0;
    for(var k=0; k<q.result.length; k++){
        if(q.result[k].n > maxn){
            maxn = q.result[k].n;
        }
    }
}

function drawPlate(overlay, ra, de, size){
    sizf = size/Math.cos(de*Math.PI/180);
    overlay.addFootprints([ A.polygon([
        [ra-sizf, de-size], 
        [ra+sizf, de-size], 
        [ra+sizf, de+size], 
        [ra-sizf, de+size] 
    ])]); 
}

function drawMarkers(data){
    console.log("drawMarkers with " + q.result.length);
    var overlay = A.graphicOverlay({color: '#ff0000', lineWidth: 2});
    aladin.addOverlay(overlay);
    for(var k=0; k<q.result.length; k++){
        row = q.result[k];
        if(row.fid == 1){
            drawPlate(overlay, row.ra, row.dec, 3.0*Math.sqrt(row.n/maxn));
        }
    }

    var overlay = A.graphicOverlay({color: '#00ff00', lineWidth: 2});
    aladin.addOverlay(overlay);
    for(var k=0; k<q.result.length; k++){
        row = q.result[k];
        if(row.fid == 2){
            drawPlate(overlay, row.ra, row.dec, 3.0*Math.sqrt(row.n/maxn));
        }
    }
}
