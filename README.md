# peersync

Sincronizador de arquivos peer-to-peer com conexão UDP.

- **Autor:** Rafael Flores Blumm
- **Disciplina:** *Redes de Computadores: Aplicação e Transporte (Unisinos 2026/2)*

## Executando a aplicação

Inicialmente, é necessário criar um arquivo YAML de configurações. Exemplo:

```yaml
sync_dir: /home/johndoe/Documents/sync
tmp_dir: /tmp/peersync
peers:
- 127.0.0.1
cache_file: /home/johndoe/Documents/peersync-cache.yaml
```

Em seguida, execute a aplicação pela linha de comando. Para iniciar
no modo sem interface gráfica, informe o parâmetro `--daemon`.

```bash
# Com interface gráfica
peersync --config /home/johndoe/Documents/peersync.yaml

# Sem interface gráfica
peersync --config /home/johndoe/Documents/peersync.yaml --daemon
```

É possível consultar os parâmetros disponíveis com `--help`:

```plaintext
$ peersync --help

Peer-to-peer UDP file sync application

Usage: peersync [OPTIONS]

Options:
  -c, --config <CONFIG>  Application settings file [default: ./config.yml]
  -d, --daemon           Start server in daemon mode, no GUI
  -h, --help             Print help
  -V, --version          Print version
```

## Design do sistema

### Arquitetura orientada a eventos

```mermaid
---
title: Event flow
---
flowchart TD
    subgraph Produtores
        csock@{ label: "Socket de controle", shape: event }
        fs@{ label: "Diretório de\nsincronização", shape: lin-cyl }
        ui@{ label: "Interface gráfica", shape: person }
        start@{ label: "Inicialização da\naplicação", shape: console }
    end

    subgraph "Publicadores (publishers)"
        ctrl@{ label: "Listener de controle", shape: subprocess }
        fwatch@{ label: "File Watcher", shape: subprocess }
        gui@{ label: "Listener da GUI", shape: subprocess }
        sync@{ label: "Sincronizador", shape: subprocess }
    end

    subgraph Orquestradores
        broker@{ label: "Broker de eventos", shape: in-out }
        router@{ label: "Roteador de eventos", shape: out-in }
    end

    subgraph "Assinantes (subscribers)"
        cfg[Atualizador de\nconfigurações]
        evt[Anunciador\nde eventos]
        rcvr[Recebedor de\narquivos]
        sendr[Enviador de\narquivos]
        tree[Enviador de\nárvore de\narquivos]
        fswrk[Worker de\nsistema de arquivos]
    end

    subgraph Atividades
        subgraph Local
            upcfg@{ label: "Atualizar arquivo de configurações", shape: terminal}
            upfs@{ label: "Atualizar diretório de sincronização", shape: terminal}
        end

        subgraph "Externo (para peers)"
            sevt@{ label: "Enviar evento", shape: terminal }
            sdt@{ label: "Enviar dados", shape: terminal }
        end
    end

    csock -. Requisição de peer .-> ctrl --> broker
    fs -. Evento de modificação de arquivo .-> fwatch --> broker
    ui -. Requisição do usuário .-> gui --> broker
    start -. Requisição da inicialização da aplicação .-> sync
    gui -. Requisição de sincronização forçada .-> sync --> broker

    broker -. Evento e seus metadados .-> router

    router --> evt -. Eventos locais de sincronização .-> sevt
    router --> sendr -. Arquivo requisitado por peer .-> sdt
    router --> tree -. Árvore de arquivos sincronizados .-> sdt
    router --> cfg -. Nova configuração .-> upcfg
    router --> rcvr -. Arquivo requisitado para peer .-> upfs
    router --> fswrk -. Manipulações gerais no sistema de arquivos .-> upfs
```

### Protocolo de comunicação

#### Portas UDP

- 5000: controle
- 5001: dados

#### Conteúdo de requisições

- 4 primeiros bytes: verbo da requisição
- Demais bytes: parâmetros separados por espaços

| Verbo | Descrição | Parâmetros |
| ----- | --------- | ---------- |
| `ACK` | Reconhecimento da requisição | *-* |
| `ADDP` | Adiciona novo peer | *Peer* |
| `RMP` | Remove peer | *Peer* |
| `TREE` | Solicita árvore de arquivos | *-* |
| `LS` | Envia listagem de arquivos | *hash + arquivo* |
| `LSE` | Finaliza envio da listagem de arquivos | *-* |
| `GET` | Solicita conteúdo de arquivo | *arquivo* |
| `CAT` | Envia conteúdo de arquivo | *arquivo + índice + conteúdo* |
| `EOF` | End of file stream | *hash* |
| `MV` | Anuncia que arquivo foi movido | *de + para* |
| `NEW` | Anuncia que arquivo foi criado | *arquivo* |
| `RM` | Anuncia que arquivo foi removido | *arquivo/diretório* |

#### Controle de retentativas

- 3 tentativas de comunicação até receber **ACK**
- 5 segundos de timeout para cada tentativa

#### Fluxo

- Peer 1 envia requisição para Peer 2
- Peer 2 responde com ACK
- Peer 2 envia dados
- Peer 1 responde com ACK
