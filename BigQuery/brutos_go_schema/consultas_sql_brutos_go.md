# Consultas SQL Básicas para o Schema brutos_go

Com base no diagrama ER, aqui estão algumas consultas SQL básicas que podem ser úteis para operações de rotina no BigQuery.

## Consultas Simples por Tabela

### 1. Empregos

```sql
-- Listar todos os empregos ativos
SELECT 
    id, 
    titulo, 
    descricao, 
    empresa_id,
    salario_min,
    salario_max,
    tipo_contratacao
FROM 
    `rj-superapp.brutos_go.empregos`
WHERE 
    status = 'ativo'
ORDER BY 
    created_at DESC
LIMIT 100;

-- Buscar empregos por faixa salarial
SELECT 
    id, 
    titulo, 
    descricao, 
    empresa_id,
    salario_min,
    salario_max
FROM 
    `rj-superapp.brutos_go.empregos`
WHERE 
    salario_min >= 3000 
    AND salario_max <= 5000
    AND status = 'ativo'
ORDER BY 
    salario_max DESC;

-- Contar empregos por empresa
SELECT 
    empresa_id,
    COUNT(*) as total_empregos
FROM 
    `rj-superapp.brutos_go.empregos`
GROUP BY 
    empresa_id
ORDER BY 
    total_empregos DESC;
```

### 2. Empresas

```sql
-- Listar todas as empresas
SELECT 
    id, 
    nome
FROM 
    `rj-superapp.brutos_go.empresas`
ORDER BY 
    nome;

-- Contar quantas vagas cada empresa tem publicada
SELECT 
    e.id,
    e.nome,
    COUNT(j.id) as total_vagas
FROM 
    `rj-superapp.brutos_go.empresas` e
LEFT JOIN 
    `rj-superapp.brutos_go.empregos` j ON e.id = j.empresa_id
WHERE 
    j.status = 'ativo'
GROUP BY 
    e.id, e.nome
ORDER BY 
    total_vagas DESC;
```

### 3. Cursos

```sql
-- Listar todos os cursos ativos
SELECT 
    id, 
    titulo, 
    theme,
    status,
    data_inicio,
    data_termino,
    vagas_totais
FROM 
    `rj-superapp.brutos_go.cursos`
WHERE 
    status = 'ativo'
    AND data_inicio > CURRENT_TIMESTAMP()
ORDER BY 
    data_inicio;

-- Buscar cursos com vagas disponíveis
SELECT 
    c.id, 
    c.titulo, 
    c.vagas_totais,
    c.vagas_totais - COUNT(i.id) AS vagas_disponiveis
FROM 
    `rj-superapp.brutos_go.cursos` c
LEFT JOIN 
    `rj-superapp.brutos_go.inscricoes` i ON c.id = i.curso_id
WHERE 
    c.status = 'ativo'
    AND c.data_inicio > CURRENT_TIMESTAMP()
GROUP BY 
    c.id, c.titulo, c.vagas_totais
HAVING 
    vagas_disponiveis > 0
ORDER BY 
    vagas_disponiveis DESC;
```

### 4. Inscrições

```sql
-- Listar todas as inscrições para um curso específico
SELECT 
    i.id,
    i.nome,
    i.email,
    i.cpf,
    i.status,
    i.enrolled_at
FROM 
    `rj-superapp.brutos_go.inscricoes` i
WHERE 
    i.curso_id = 123  -- Substituir pelo ID do curso desejado
ORDER BY 
    i.enrolled_at;

-- Contar inscrições por status
SELECT 
    status,
    COUNT(*) as total
FROM 
    `rj-superapp.brutos_go.inscricoes`
GROUP BY 
    status
ORDER BY 
    total DESC;
```

## Consultas com Joins

### 1. Empregos com Detalhes da Empresa e Escolaridade

```sql
-- Listar empregos com detalhes da empresa e escolaridade
SELECT 
    e.id AS emprego_id,
    e.titulo,
    e.descricao,
    e.salario_min,
    e.salario_max,
    emp.nome AS empresa_nome,
    esc.nivel AS nivel_escolaridade
FROM 
    `rj-superapp.brutos_go.empregos` e
JOIN 
    `rj-superapp.brutos_go.empresas` emp ON e.empresa_id = emp.id
JOIN 
    `rj-superapp.brutos_go.escolaridades` esc ON e.escolaridade_id = esc.id
WHERE 
    e.status = 'ativo'
ORDER BY 
    e.created_at DESC
LIMIT 100;
```

### 2. Cursos com Categorias

```sql
-- Listar cursos com suas categorias
SELECT 
    c.id AS curso_id,
    c.titulo AS curso_titulo,
    c.status,
    c.data_inicio,
    c.data_termino,
    STRING_AGG(cat.nome, ', ') AS categorias
FROM 
    `rj-superapp.brutos_go.cursos` c
JOIN 
    `rj-superapp.brutos_go.cursos_categorias` cc ON c.id = cc.curso_id
JOIN 
    `rj-superapp.brutos_go.categorias` cat ON cc.categoria_id = cat.id
WHERE 
    c.status = 'ativo'
GROUP BY 
    c.id, c.titulo, c.status, c.data_inicio, c.data_termino
ORDER BY 
    c.data_inicio;
```

### 3. Cursos com Detalhes de Acessibilidade

```sql
-- Listar cursos com suas acessibilidades
SELECT 
    c.id AS curso_id,
    c.titulo AS curso_titulo,
    STRING_AGG(a.nome, ', ') AS acessibilidades
FROM 
    `rj-superapp.brutos_go.cursos` c
JOIN 
    `rj-superapp.brutos_go.cursos_acessibilidades` ca ON c.id = ca.curso_id
JOIN 
    `rj-superapp.brutos_go.acessibilidades` a ON ca.acessibilidade_id = a.id
WHERE 
    c.status = 'ativo'
GROUP BY 
    c.id, c.titulo
ORDER BY 
    c.titulo;
```

### 4. Cursos com Informações de Local

```sql
-- Listar cursos presenciais com detalhes de localização
SELECT 
    c.id AS curso_id,
    c.titulo AS curso_titulo,
    c.status,
    c.data_inicio,
    c.data_termino,
    lc.address,
    lc.neighborhood,
    lc.class_days,
    lc.class_time,
    lc.vacancies
FROM 
    `rj-superapp.brutos_go.cursos` c
JOIN 
    `rj-superapp.brutos_go.location_classes` lc ON c.id = lc.curso_id
WHERE 
    c.status = 'ativo'
ORDER BY 
    c.data_inicio;

-- Listar cursos remotos
SELECT 
    c.id AS curso_id,
    c.titulo AS curso_titulo,
    c.status,
    c.data_inicio,
    c.data_termino,
    rc.class_days,
    rc.class_time,
    rc.vacancies
FROM 
    `rj-superapp.brutos_go.cursos` c
JOIN 
    `rj-superapp.brutos_go.remote_classes` rc ON c.id = rc.curso_id
WHERE 
    c.status = 'ativo'
ORDER BY 
    c.data_inicio;
```

## Consultas Avançadas

### 1. Estatísticas de Inscrições por Curso

```sql
-- Estatísticas de inscrições por curso
SELECT 
    c.id AS curso_id,
    c.titulo AS curso_titulo,
    c.vagas_totais,
    COUNT(i.id) AS total_inscricoes,
    c.vagas_totais - COUNT(i.id) AS vagas_restantes,
    ROUND(COUNT(i.id) / c.vagas_totais * 100, 2) AS percentual_preenchimento
FROM 
    `rj-superapp.brutos_go.cursos` c
LEFT JOIN 
    `rj-superapp.brutos_go.inscricoes` i ON c.id = i.curso_id
WHERE 
    c.status = 'ativo'
GROUP BY 
    c.id, c.titulo, c.vagas_totais
ORDER BY 
    percentual_preenchimento DESC;
```

### 2. Cursos por Órgão e Instituição

```sql
-- Contar cursos por órgão e instituição
SELECT 
    o.nome AS orgao_nome,
    ie.nome AS instituicao_nome,
    COUNT(c.id) AS total_cursos
FROM 
    `rj-superapp.brutos_go.cursos` c
JOIN 
    `rj-superapp.brutos_go.orgaos` o ON c.orgao_id = o.id
JOIN 
    `rj-superapp.brutos_go.instituicoes_ensino` ie ON c.instituicao_id = ie.id
GROUP BY 
    o.nome, ie.nome
ORDER BY 
    total_cursos DESC;
```

### 3. Empregos por Nível de Escolaridade e Faixa Salarial

```sql
-- Análise de empregos por escolaridade e faixa salarial
SELECT 
    esc.nivel AS nivel_escolaridade,
    CASE 
        WHEN e.salario_max <= 2000 THEN 'Até R$2.000'
        WHEN e.salario_max <= 4000 THEN 'R$2.001 - R$4.000'
        WHEN e.salario_max <= 6000 THEN 'R$4.001 - R$6.000'
        WHEN e.salario_max <= 8000 THEN 'R$6.001 - R$8.000'
        WHEN e.salario_max <= 10000 THEN 'R$8.001 - R$10.000'
        ELSE 'Acima de R$10.000'
    END AS faixa_salarial,
    COUNT(*) AS total_vagas
FROM 
    `rj-superapp.brutos_go.empregos` e
JOIN 
    `rj-superapp.brutos_go.escolaridades` esc ON e.escolaridade_id = esc.id
WHERE 
    e.status = 'ativo'
GROUP BY 
    nivel_escolaridade, faixa_salarial
ORDER BY 
    nivel_escolaridade, faixa_salarial;
```

### 4. Análise Temporal de Inscrições

```sql
-- Análise de inscrições ao longo do tempo
SELECT 
    DATE(i.enrolled_at) AS data_inscricao,
    COUNT(*) AS total_inscricoes
FROM 
    `rj-superapp.brutos_go.inscricoes` i
WHERE 
    i.enrolled_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY 
    data_inscricao
ORDER BY 
    data_inscricao;
```

## Consultas para Administração

### 1. Verificação de Integridade de Dados

```sql
-- Verificar cursos sem categorias
SELECT 
    c.id,
    c.titulo
FROM 
    `rj-superapp.brutos_go.cursos` c
LEFT JOIN 
    `rj-superapp.brutos_go.cursos_categorias` cc ON c.id = cc.curso_id
WHERE 
    cc.curso_id IS NULL
    AND c.status = 'ativo';

-- Verificar empregos sem empresa válida
SELECT 
    e.id,
    e.titulo
FROM 
    `rj-superapp.brutos_go.empregos` e
LEFT JOIN 
    `rj-superapp.brutos_go.empresas` emp ON e.empresa_id = emp.id
WHERE 
    emp.id IS NULL
    AND e.status = 'ativo';
```

### 2. Atualização de Status

```sql
-- Atualizar status de cursos expirados
UPDATE 
    `rj-superapp.brutos_go.cursos`
SET 
    status = 'encerrado'
WHERE 
    data_termino < CURRENT_TIMESTAMP()
    AND status = 'ativo';

-- Atualizar status de vagas expiradas
UPDATE 
    `rj-superapp.brutos_go.empregos`
SET 
    status = 'fechado'
WHERE 
    data_limite_candidatura < CURRENT_TIMESTAMP()
    AND status = 'ativo';
```

Estas consultas podem ser adaptadas conforme necessário para atender às suas necessidades específicas de negócio e análise de dados.