// Chart.js scripts
// -- Set new default font family and font color to mimic Bootstrap's default styling
Chart.defaults.global.defaultFontFamily = '-apple-system,system-ui,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif';
Chart.defaults.global.defaultFontColor = '#292b2c';


// -- Bar Chart Example
var ctx = document.getElementById("daily_energy_chart");
var myLineChart = new Chart(ctx, {
  type: 'bar',
  data: {
    labels: daily_log_elec_time,
    datasets: [{
      label: "daily elec used",
      backgroundColor: '#dc3545',
      borderColor: '#dc3545',
      data: daily_log_elec_consumed,
    },
	{
      label: "daily elec returned",
      backgroundColor: "rgba(2,117,216,1)",
      borderColor: "rgba(2,117,216,1)",
      data: daily_log_elec_returned,
    },
	{
      label: "daily solar energy",
      backgroundColor: '#28a745',
      borderColor: '#28a745',
      data: daily_log_elec_generated,
    }],
  },
  options: {
    scales: {
      xAxes: [{
        time: {
          unit: 'month'
        },
        gridLines: {
          display: false
        },
        ticks: {
          maxTicksLimit: 6
        }
      }],
      yAxes: [{
          gridLines: {
          display: true
        },
	scaleLabel: {
	  display: true,
	  labelString: "[kWh]",
	}
      }],
    },
    legend: {
      display: true
    }
  }
});



// -- Bar Chart Example
var ctx = document.getElementById("gas_log");
var myLineChart = new Chart(ctx, {
  type: 'bar',
  data: {
    labels: daily_log_gas_time,
    datasets: [{
      label: "Gas used",
      backgroundColor: '#dc3545',
      borderColor: '#dc3545',
      data: daily_log_gas_used,
    }],
  },
  options: {
    scales: {
      xAxes: [{
        time: {
          unit: 'month'
        },
        gridLines: {
          display: false
        },
        ticks: {
          maxTicksLimit: 6
        }
      }],
      yAxes: [{
          gridLines: {
          display: true
        },
	scaleLabel: {
	  display: true,
	  labelString: "[m³]",
	}
      }],
    },
    legend: {
      display: true
    }
  }
});


// -- Bar Chart Example
var ctx = document.getElementById("daily_cost_chart");
var myLineChart = new Chart(ctx, {
  type: 'bar',
  data: {
    labels: daily_log_cost_time,
    datasets: [{
      label: "daily cost prosument formula",
      backgroundColor: "rgba(192,192,192,1)",
      borderColor: "rgba(192,192,192,1)",
      data: daily_log_cost_prosument,
    },
	{
      label: "daily cost smart formula",
      backgroundColor: "rgba(128,128,128,1)",
      borderColor: "rgba(128,128,128,1)",
      data: daily_log_cost_smart,
    }
  ],
  },
  options: {
    scales: {
      xAxes: [{
        time: {
          unit: 'month'
        },
        gridLines: {
          display: false
        },
        ticks: {
          maxTicksLimit: 6
        }
      }],
      yAxes: [{
          gridLines: {
          display: true
        },
	scaleLabel: {
	  display: true,
	  labelString: "[€]",
	}
      }],
    },
    legend: {
      display: true
    }
  }
});
