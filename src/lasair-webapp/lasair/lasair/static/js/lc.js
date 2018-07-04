function plotlc(data){
gmag = []
gt = []
gerror = []
gra = []
gdec = []
rmag = []
rt = []
rerror = []
rra = []
rdec = [] 
g = 'rgb(104,139,46)';
r = 'rgb(244,2,52)'; 
first_item = data[0];
first_ra = Number(first_item.ra)*3600;
first_dec = Number(first_item.decl)*3600;

data.forEach(function(item){
	y = Number(item.magpsf);
        x = Number(item.jd)-2400000.5;
	e = Number(item.sigmapsf);
	x2 = first_ra - Number(item.ra)*3600;
	y2 = first_dec - Number (item.decl)*3600;
	fid = Number(item.fid);
	if(fid == 1){
		gmag.push(y);
		gt.push(x);
		gerror.push(e);
		gra.push(x2);
		gdec.push(y2);
	}
	else if(fid ==2){
		rmag.push(y);
       		rt.push(x);
        	rerror.push(e);
        	rra.push(x2);
        	rdec.push(y2);
	}
});



lc_div = document.getElementById('lc');
var lcg = {x:gt, y: gmag, error_y:{
		type:'data',
		color: g,
		opacity: 0.7,
		array: gerror,
		visible: true
		},
	mode:'markers',
	marker: {
		color:g
	},
	type:'scatter'
	}
var lcr = {x:rt, y: rmag, error_y:{
                type:'data',
		color: r,
                array: rerror,
		opactity: 0.7,
                visible: true},
        mode:'markers',
	marker: {
                color:r
        },
        type:'scatter'
        }

Plotly.plot(lc_div, [lcg, lcr], {
	margin: { t: 0 }, 
	displayModeBar: false, 
	showlegend: false,
	xaxis: {
		title:'MJD',
		tickformat :".f" },
	yaxis: {
		title: 'Magnitude',
		autorange: 'reversed'	
	}
	 },
	{displayModeBar: false});

radec_div = document.getElementById('radec');

var radecg = {x:gra, y: gdec,
        mode:'markers',
 	marker: {
                color:'rgb(104,139,46)'
        },
        type:'scatter'
        }

var radecr = {x:rra, y: rdec,
        mode:'markers',
	marker: {
                color:'rgb(244,2,52)'
        },
        type:'scatter'
        }

Plotly.plot(radec_div, [radecg, radecr], {
	margin: { t: 0 },
	showlegend: false,
	shapes: [
        {
            type: 'circle',
            xref: '0',
            yref: '0',
            x0: -1.5,
            y0: -1.5,
            x1: 1.5,
            y1: 1.5,
            opacity: 0.3,
            fillcolor: '#bbded6',
            line: {
                color: 'black'
            }
        }]
}, {displayModeBar: false}
);

}
