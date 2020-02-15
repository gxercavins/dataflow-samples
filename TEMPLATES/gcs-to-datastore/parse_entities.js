function transform(elem) {
	var data = JSON.parse(elem);

	var entity = {};
	entity.key = {};
	entity.key.partitionId = {};
	entity.key.partitionId.projectId = "gcp-project-id";
	entity.key.partitionId.namespaceId = "spring-demo";

	var path = {}
	path.kind = "demo";
	path.name = "userId";
	entity.key.path = [];
	entity.key.path.push(path);

	entity.properties = {};
	entity.properties.userId = {};
	entity.properties.userId.stringValue = data.userId;
	entity.properties.rowRanks = {};
	entity.properties.rowRanks.arrayValue = {};

	var arrayValues = [];
	data.rowRanks.forEach(buildArrayValue);

	function buildArrayValue(row) {
	  var temp = {};
	  temp.entityValue = {};
	  temp.entityValue.properties = {};
	  temp.entityValue.properties.originalTrigger = {};
	  temp.entityValue.properties.originalTrigger.stringValue = row.originalTrigger;
	  temp.entityValue.properties.programmedRowPos = {};
	  temp.entityValue.properties.programmedRowPos.stringValue = row.programmedRowPos;
	  temp.entityValue.properties.reorderedRowPos = {};
	  temp.entityValue.properties.reorderedRowPos.integerValue = row.reorderedRowPos;
	  arrayValues.push(temp);
	}

	entity.properties.rowRanks.arrayValue.values = arrayValues;

	return JSON.stringify(entity);
}