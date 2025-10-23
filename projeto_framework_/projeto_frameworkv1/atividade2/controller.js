// Simulação de consumo da API ViaCEP (sem chamada real)
console.log('Script controller.js carregado');

function buscarCEP(){
  console.log('Função buscarCEP chamada');
  var cepInput = document.getElementById('cep');
  var logElement = document.getElementById('log');
  
  if(!cepInput || !logElement){
    console.error('Elementos do formulário não encontrados');
    return;
  }
  
  var cep = cepInput.value.replace(/\D/g,'');
  console.log('CEP inserido:', cep);
  
  if(!cep || cep.length !== 8){
    logElement.textContent = 'CEP inválido. Insira 8 dígitos.';
    console.log('CEP inválido');
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
  
  console.log('Preenchendo campos com:', respostaSimulada);
  document.getElementById('rua').value = respostaSimulada.logradouro;
  document.getElementById('bairro').value = respostaSimulada.bairro;
  document.getElementById('cidade').value = respostaSimulada.localidade;
  document.getElementById('estado').value = respostaSimulada.uf;
  logElement.textContent = 'Resposta (simulada) recebida: ' + JSON.stringify(respostaSimulada, null, 2);
  console.log('Campos preenchidos com sucesso');
}

// Aguarda o DOM estar completamente carregado
document.addEventListener('DOMContentLoaded', function(){
  console.log('DOM carregado');
  var botao = document.getElementById('buscar-cep');
  if(botao){
    botao.addEventListener('click', buscarCEP);
    console.log('Evento de clique adicionado ao botão');
  } else {
    console.error('Botão buscar-cep não encontrado');
  }
});
