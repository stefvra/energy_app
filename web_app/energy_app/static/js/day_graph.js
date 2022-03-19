// -- Area Chart Example
var ctx = document.getElementById("power_log");
var power_chart = new Chart(ctx, {
  type: 'line',
  data: {
    labels: today_log_time,
    datasets: [{
      label: "Solar Power",
      lineTension: 0.3,
      borderColor: '#28a745',
      borderWidth: 2,
      pointRadius: 0,
      pointBackgroundColor: '#28a745',
      pointBorderColor: "rgba(255,255,255,0.8)",
      pointHoverBackgroundColor: '#28a745',
      pointHitRadius: 20,
      pointBorderWidth: 1,
      data: today_log_solar_power,
    },
	{
      label: "Consumption",
      lineTension: 0.3,
      borderColor: "rgba(2,117,216,1)",
      borderWidth: 2,
      pointRadius: 0,
      pointBackgroundColor: "rgba(2,117,216,1)",
      pointBorderColor: "rgba(255,255,255,0.8)",
      pointHoverBackgroundColor: "rgba(2,117,216,1)",
      pointHitRadius: 20,
      pointBorderWidth: 1,
      data: today_log_consumption,
    }],
  },
  options: {
    scales: {
      xAxes: [{
        time: {
          unit: 'date'
        },
      }],
      yAxes: [{
        gridLines: {
          color: "rgba(0, 0, 0, .125)",
        },
	scaleLabel: {
	  display: true,
	  labelString: "[kW]",
	}
      }],
    },
    legend: {
      display: true
    },
  }
});