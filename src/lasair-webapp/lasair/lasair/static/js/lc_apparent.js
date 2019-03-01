function plotlc_apparent(data){
gmag = []
gt = []
gerror = []

rmag = []
rt = []
rerror = []

g = 'rgb(104,139,46)';
r = 'rgb(244,2,52)'; 
candidates = data.candidates;

candidates.forEach(function(item){
    y = Number(item.dc_mag);
    x = Number(item.mjd);
    e = Number(item.dc_sigmag);
    fid = Number(item.fid);
    det = (item.candid)
    if(det){
        if(fid == 1){
            gmag.push(y);
            gt.push(x);
            gerror.push(e);
        }
        else if(fid ==2){
            rmag.push(y);
            rt.push(x);
            rerror.push(e);
        }
    }
});

lc_div = document.getElementById('lc_apparent');
var lcg = {x:gt, y: gmag, error_y:{
        type:'data',
        color: g,
        opacity: 0.7,
        array: gerror,
        visible: true
        },
    mode:'markers',
    marker: { color:g, size: 8 },
    type:'markers'
}
var lcr = {x:rt, y: rmag, error_y:{
        type:'data',
        color: r,
        array: rerror,
        opacity: 0.7,
        visible: true
        },
        mode:'markers',
    marker: { color:r, size: 8 },
        type:'markers'
}

Plotly.plot(lc_div, [lcg, lcr], {
    margin: { t: 0 }, 
    displayModeBar: false, 
    showlegend: false,
    xaxis: {
        title:'MJD',
        tickformat :".f" },
    yaxis: {
        title: 'Apparent Magnitude',
        autorange: 'reversed'    
    }
     },
    {displayModeBar: false});
}
