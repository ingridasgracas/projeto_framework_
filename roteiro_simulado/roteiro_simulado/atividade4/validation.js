$(function(){
  $('#cadCliente').on('submit', function(e){
    e.preventDefault();
    var nome = $('#nome').val().trim();
    var email = $('#email').val().trim();
    var cep = $('#cep').val().replace(/\D/g,'');
    var resultado = $('#resultado');
    var emailRegex = /^[\w\.\-]+@[\w\-]+(\.[A-Za-z]{2,})+$/;
    if(!nome){ resultado.text('Nome é obrigatório.'); return; }
    if(!emailRegex.test(email)){ resultado.text('Email inválido.'); return; }
    if(!cep || cep.length !== 8){ resultado.text('CEP inválido.'); return; }
    // Simula busca via CEP e sucesso
    resultado.text('Validação OK. Simulando submissão...\n' + JSON.stringify({nome:nome,email:email,cep:cep}, null, 2));
  });
});
