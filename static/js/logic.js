// d3.json("/api/v1.0/data").then((data)=>{
  //console.log(data.avo_transport)
// });

  // Define sections of data
  // var avo_prices = data.avocado_prices
  // var gas_prices = data.gas_prices
  // var tot_transport = data.tot_transport
  // var avo_transport = data.avo_transport
  // var weather = data.weather
  // var weather2 = data.weather2
  // var bananas = data.bananas
  // console.log(avo_prices)
  // console.log(gas_prices)
  // console.log(tot_transport)
  // console.log(avo_transport)
  // console.log(weather)
  // console.log(bananas)

// -- FUNCTION TO HIDE TITLE BLOCK --
$(document).ready(function(){

  $('#hide').click(function(){
  $('#titleblock').toggle(1500);
    
  });
    
});
// -- FUNCTION TO HIDE TITLE BLOCK --

  // initialize charts for rewriting with input data
function init() {
    
    // Set up chart
    var trace = {
      x: [],
      y: [],
      mode: 'markers',
      marker: {
        size: []
      }
    };
    
    var data = [trace];
    
    var layout = {
      title: 'Charts',
      showlegend: false,
    };
    
    var chart = d3.select("#chart").node();
    
    Plotly.newPlot(chart, data, layout);
}

// -- TRANSPORT DATA FUNCTIONS AND INFO --
d3.select(".avocado__skin").on("click", transport);

  function transport() {
    // console.log("it worked");
    d3.json("/api/v1.0/transport").then((data)=>{
      console.log(JSON.parse(JSON.stringify(data)))
      // var tot_transport = data.tot
      // var avo_transport = data.avo
      // var avo_prices = data.price
      // console.log(tot_transport)

      // var date = tot_transport.map(row => row.date)
      // console.log(date)

      // replotting transport visualizations
      // var avoTransportTrace1 = {
      //   x: [],
      //   y: [],
      //   mode: 'markers',
      //   marker: {
      //     size: []
      //   }
      // };

      // var avoTransportTrace2 = {
      //   x: [],
      //   y: [],
      //   mode: 'markers',
      //   marker: {
      //     size: []
      //   }
      // };

      // var avoTransportTrace3 = {
      //   x: [],
      //   y: [],
      //   mode: 'markers',
      //   marker: {
      //     size: []
      //   }
      // };
    
      // var transportData = [avoTransportTrace1, avoTransportTrace2, avoTransportTrace3];
    
      // var transportlayout = {
      //   title: 'Transport Data Analysis',
      //   showlegend: false,
      // };
    
      // var transportChart = d3.select("#chart").node();
    
      // Plotly.react(transportChart, transportData, transportlayout);
    
      // // grab nodes for updating info card
      // var dashboard_title = d3.select("#dashboard_title");
      // var dashboard_subTitle = d3.select("#dashboard_subtitle");
      // var dashboard_text = d3.select("#dashboard_text");
    
      // // Update info card with new text
      // dashboard_title.text("USDA Transport Data");
      // dashboard_subTitle.text("Trucking Availability and Rates");
      // dashboard_text.text("This data comes from the USDA Ag Transport site https://agtransport.usda.gov/, specifically the trucking category. This data shows the availability and rates for refrigerated trucks transporting ag commodities and we utilized this and were able to drill down to see costs associated specifically with avocados.");
    });      
};
// -- TRANSPORT DATA FUNCTIONS AND INFO --

// -- WEATHER DATA FUNCTIONS AND INFO --
d3.select(".avocado__inner").on("click", weather);

  function weather() {
    // console.log("it worked");
    d3.json("/api/v1.0/data").then((data)=>{
      // replotting weather visualizations
      
      var weatherTrace = {
        x: [],
        y: [],
        mode: 'markers',
        marker: {
          size: []
        }
      };
      
      var weatherData = [weatherTrace];
    
      var weatherLayout = {
        title: 'Weather Data Analysis',
        showlegend: false,
      };
    
      var chart = d3.select("#chart").node();
    
      Plotly.react(chart, weatherData, weatherLayout);
    
      // grab nodes for updating info card
      var dashboard_titleW = d3.select("#dashboard_title");
      var dashboard_subTitleW = d3.select("#dashboard_subtitle");
      var dashboard_textW = d3.select("#dashboard_text");
    
      // Update info card with new text
      dashboard_titleW.text("Open Weather API");
      dashboard_subTitleW.text("San Diego Historical Weather");
      dashboard_textW.text("Info on this data, limitations, etc.");
    }); 
  };
// -- WEATHER DATA FUNCTIONS AND INFO --

// -- GAS DATA FUNCTIONS AND INFO --
d3.select(".avocado__inner-shadow").on("click", gas);

  function gas() {
    // console.log("it worked");
    d3.json("/api/v1.0/data").then((data)=>{
    // replotting gas visualizations
    
      var gasTrace = {
        x: [],
        y: [],
        mode: 'markers',
        marker: {
          size: []
        }
      };
    
      var gasData = [gasTrace];
    
      var gasLayout = {
        title: 'Gas Data Analysis',
        showlegend: false,
      };
    
      var chart = d3.select("#chart").node();
    
      Plotly.react(chart, gasData, gasLayout);
    
      // grab nodes for updating info card
      var dashboard_titleG = d3.select("#dashboard_title");
      var dashboard_subTitleG = d3.select("#dashboard_subtitle");
      var dashboard_textG = d3.select("#dashboard_text");
    
      // Update info card with new text
      dashboard_titleG.text("US Energy Information Administration Data");
      dashboard_subTitleG.text("Gas Prices Data");
      dashboard_textG.text("Info on this data, limitations, etc.");
    });
  };
// -- GAS DATA FUNCTIONS AND INFO --

// -- BANANA DATA FUNCTIONS AND INFO --
d3.select(".avocado__seed").on("click", banana);

  function banana() {
    // console.log("it worked");
    d3.json("/api/v1.0/data").then((data)=>{
    // replotting banana visualizations
      var avocado_prices = data.avocado_prices
      var banana_prices = data.bananas
      var gas_prices = data.gas_prices
      
      var bananaTrace = {
        x: banana_prices[0].date,
        y: banana_prices[0].price_per_pound,
        mode: 'lines',
        type: 'scatter',
        name: "Bananas",
        line: {
          color: 'yellow'
        }
      };
      var gasTrace = {
        x: gas_prices[0].date,
        y: gas_prices[0].gas_all_grades,
        mode: 'lines',
        type: 'scatter',
        name: 'Gas',
        line: {
          color: 'black'
        }
      };
      var avocadoTrace = {
        x: avocado_prices[0].date,
        y: avocado_prices[0].average_price,
        mode: 'lines',
        type: 'scatter',
        name: 'Avocado',
        line: {
          color: 'green'
        }
      };
      var compareData = [avocadoTrace, bananaTrace, gasTrace];
      var compareLayout = {
        title: 'Overall Price Analysis',
        showlegend: false,
        xaxis: {
          range: ['01/01/2015', '03/31/2018'],
          type: 'date'
        },
        yaxis: {
          range: [0, 5],
          type: 'linear'
        }
      };
      var chart = d3.select("#chart").node();
      Plotly.react(chart, compareData, compareLayout);
    
      // grab nodes for updating info card
      var dashboard_titleB = d3.select("#dashboard_title");
      var dashboard_subTitleB = d3.select("#dashboard_subtitle");
      var dashboard_textB = d3.select("#dashboard_text");
    
      // Update info card with new text
      dashboard_titleB.text("USDA Economic Research Service Data");
      dashboard_subTitleB.text("Banana Prices Data");
      dashboard_textB.text("Info on this data, limitations, etc.");
    });
};
// -- BANANA DATA FUNCTIONS AND INFO --

init();
