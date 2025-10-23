// Simulação de consumo da API ViaCEP (sem chamada real)
document.getElementById('buscar-cep').addEventListener('click', function(){
  var cep = document.getElementById('cep').value.replace(/\D/g,'');
  var log = document.getElementById('log');
  if(!cep || cep.length !== 8){
    log.textContent = 'CEP inválido. Insira 8 dígitos.';
    return;
  }
  // Simulando resposta da ViaCEP
  var respostaSimulada = {
    "cep":"01001-000",
    "logradouro":"Praça da Sé",
    "complemento":"",
    "bairro":"Sé",
    "localidade":"São Paulo",
    "uf":"SP"
  };
  document.getElementById('rua').value = respostaSimulada.logradouro;
  document.getElementById('bairro').value = respostaSimulada.bairro;
  document.getElementById('cidade').value = respostaSimulada.localidade;
  document.getElementById('estado').value = respostaSimulada.uf;
  log.textContent = 'Resposta (simulada) recebida: ' + JSON.stringify(respostaSimulada, null, 2);
});
