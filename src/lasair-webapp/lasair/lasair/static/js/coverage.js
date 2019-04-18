var aladin;
var storedData = {};
var maxn;

function setupAladin(){
    console.log("Setting up Aladin");
    var fov = 180;   // field of view in degrees
    aladin = $.aladin("#aladin-lite-div", {survey: "P/Mellinger/color", zoom: fov, showGotoControl: false});
    aladin.gotoRaDec(285.0, 30.0);

//    aladin.setProjection("aitoff");    
    aladin.setProjection("sinus");    
}

function queryCoverage(passnid1, passnid2) {
    nid1 = passnid1;
    nid2 = passnid2;

    jsonurl = "/coverageAjax/" + nid1 + "/" + nid2 + "/";
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
    if("result" in data) {
        console.log("Found " + data.result.length + " entries");
    }
    storedData = data;
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

function drawMarkers(){
    drawMarkersFromData(storedData.result)
}

function drawMarkersFromData(data){
    console.log("drawMarkers with " + data.length);
    maxn = 0;
    for(var k=0; k<data.length; k++){
        if(data[k].n > maxn){
            maxn = data[k].n;
        }
    }
    var overlay = A.graphicOverlay({color: "#ff0000", lineWidth: 2});
    aladin.addOverlay(overlay);
    for(var k=0; k<data.length; k++){
        row = data[k];
        if(row.fid == 1){
console.log(row.ra +" | "+ row.dec);
            drawPlate(overlay, row.ra, row.dec, 3.0*Math.sqrt(row.n/maxn));
        }
    }

    var overlay = A.graphicOverlay({color: "#00ff00", lineWidth: 2});
    aladin.addOverlay(overlay);
    for(var k=0; k<data.length; k++){
        row = data[k];
        if(row.fid == 2){
            drawPlate(overlay, row.ra, row.dec, 3.0*Math.sqrt(row.n/maxn));
        }
    }
}

function writeCoverageTable(){
    var total = 0;
    for(var k=0; k<storedData.result.length; k++){
        total += storedData.result[k].n;
    }
    console.log("candidates: " + total);
    html  = "Total candidate alerts = " + total;
    html += "<table id=fields_table class='table'>";
    html += "<thead><tr><th>Field</th><th>RA</th><th>Dec</th><th>Filter</th><th>Ncandidate</th></tr></thead><tbody>";
    for(var k=0; k<storedData.result.length; k++){
        row = storedData.result[k];
        if(row.fid == 1) {tok = "r";}
        else             {tok = "g";}
        html += "<tr><td>" + row.field + "</td><td>" + row.ra + "</td><td>" +  row.dec + "</td><td>" + tok + "</td><td>" +row.n + "</td></tr>";
    }
    html += "</tbody></table>";
    document.getElementById("coverageTable").innerHTML = html;
    $("#fields_table").tablesorter(); 
}
