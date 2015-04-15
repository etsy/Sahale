var Kiwi = (function ($) {
	var interpolate_general = function(text, array){
		var stringToReturn = ""
			,length = text.length
			,index = 0
			,array_length = array.length;

		for(var i = 0; i < length; i++){
			var currentChar = text[i];
			var nextChar = null;
			if(i + 1 < length) nextChar = text[i+1];

			if(currentChar === '`' && nextChar === '%'){
				stringToReturn += '%';
				i += 1;
			}else{
				if(currentChar === '%'){
					if(index < array_length){
						stringToReturn += array[index++];
					}else{
						stringToReturn += '';
					}
				}
				else{
					stringToReturn += currentChar;
				}
			}
		}
		return stringToReturn;
	}

	var get_key_length = function(text, index){
		var length = text.length;

		var key = "";

		var i = index;
		while(i < length && text[i] !== '}'){
			key += text[i];
			i++;
		}

		if(text[i] === '}') key += '}';
		else throw Error("SYNTAX ERROR");

		return [key.slice(2, key.length - 1), key.length];
	} 

	var interpolate_key_value = function(text, json){
		var stringToReturn = ""
			,length = text.length
			,index = 0;

		for(var i = 0; i < length; i++){
			var currentChar = text[i];
			var nextChar = null;
			if(i + 1 < length) nextChar = text[i+1];

			if (currentChar === '`' && nextChar === '%') {
				stringToReturn += '%';
				i += 1;
			}else if(currentChar === '%' && nextChar === '{'){
				var result = get_key_length(text, i);
				var key = result[0];
				var key_length = result[1];

				stringToReturn += (json[key] === undefined ? "" : json[key]);
				i += key_length - 1 ;

			}else{
				stringToReturn += currentChar;
			}
		}

		return stringToReturn;
	}

	var interpolate = function(text, input){
		if(input === undefined) return text;

		if(Object.prototype.toString.call(input) === "[object Array]"){
			return interpolate_general(text, input);
		}else{
			return interpolate_key_value(text, input);
		}
	};

	return {
		"compose": interpolate
	};

}(Kiwi));