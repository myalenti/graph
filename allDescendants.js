var oneMatch={ "$match" : { "DUNS_NBR" : "100000011" } };
var oneLookup={ "$graphLookup" : { 
	from: "tv", 
	startWith : "$DUNS_NBR", 
	connectFromField : "DUNS_NBR", 
	connectToField : "ASSN.DUNS_NBR", 
	as : "Descendants",
	depthField : "depth"} }; 

var oneProject={ "$project" : {
	"_id" : 0,
	"Source" : "$DUNS_NBR" , 
	"Descendants.DUNS_NBR" : 1,
	"Descendants.depth" : 1}
};

var oneUnwind={ "$unwind" : "$Descendants" };
var oneSort={ "$sort" : { "Descendants.depth" : 1 , "Descendants.DUNS_NBR" : 1 }};

var result=db.tv.aggregate([ oneMatch, oneLookup, oneProject, oneUnwind, oneSort  ]);
result.forEach(printjson)
