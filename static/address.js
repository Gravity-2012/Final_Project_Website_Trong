

var searchnames= [];
  /////////////////////////////////////////////////UPDATE

console.log(searchnames);
//   /////////////////////////////////////////////////UPDATE

  d3.json('/addresses').then(data => {
    console.log(data);
    var addresses = data.map(data => data.address);
    console.log(addresses);
    const uniqueSet= new Set(addresses);
    const backToArray= [...uniqueSet];
    console.log(backToArray.length);
    //Push names to searchnames global variable, to be read in to search.js
    
    for (var i=0; i<backToArray.length;i++){
      searchnames.push(backToArray[i]);
    };

//Select the Select button in order to pull the input address
var button = d3.select("#button");
button.on("click", function grab() {
var inputvalue=[];
//Clear Table of default or filtered data
var input = document.getElementById("myAddress").value;
inputvalue.push(input);
console.log(inputvalue);

//Function to filter data by the Input Value
function address(x){return (x.address ===inputvalue[0]);}

//Store filtered data in chosen variable 
    var chosen = data.filter(address);
    console.log(chosen[0].account_number);
    // var account_chosen= (Object.values(chosen[0].account_number));
    // console.log(account_chosen);
    
//Test filter with the chosen account_number by filtering data by the account number to return the address
    function acct(x){return (x.account_number ===chosen[0].account_number);}
    var blah = data.filter(acct);
    console.log(blah[0].address);
    var blahblah= blah[0].address;
    console.log(blahblah);
});

});
//Autocomplte Function
  $("#myAddress").autocomplete({ 
    maxResults: 5,
    source: function(request, response) {
        var results = $.ui.autocomplete.filter(searchnames, request.term);
        
        response(results.slice(0, this.options.maxResults));
    }
});


 