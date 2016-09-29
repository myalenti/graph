db.tv.update( { "GEO_REF_ID" : { $lt : 100 }, $where : "this.DUNS_NBR == this.ASSN.DUNS_NBR"}, { $set : { "ASSN.DUNS_NBR" : null }}, { multi : true} )
