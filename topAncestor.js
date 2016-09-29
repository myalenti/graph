var oneMatch={ "$match" : { "DUNS_NBR" : "100000025" } };
var oneLookup={ "$graphLookup" : { 
	from: "tv", 
	startWith : "$ASSN.DUNS_NBR", 
	connectFromField : "ASSN.DUNS_NBR", 
	connectToField : "DUNS_NBR", 
	as : "Ancestors",
	depthField : "depth"} }; 

var oneProject={ "$project" : {
	"_id" : 0,
	"Source" : "$DUNS_NBR" , 
	"Ancestors.DUNS_NBR" : 1,
	"Ancestors.depth" : 1}
};

var oneUnwind={ "$unwind" : "$Ancestors" };
var oneSort={ "$sort" : { "Ancestors.depth" : -1 , "Ancestors.DUNS_NBR" : 1 }};
var oneLimit={ "$limit" : 1 };
var renameFields={ "$project" : { "Source" : 1 , "ultimateParent" : "$Ancestors.DUNS_NBR"}}

var result=db.tv.aggregate([ oneMatch, oneLookup, oneProject, oneUnwind, oneSort, oneLimit, renameFields  ]);
result.forEach(printjson)
