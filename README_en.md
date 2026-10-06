# peersync

UDP P2P file sync

## System design

### Event-driven architecture

```mermaid
---
title: Event flow
---
flowchart TD
    subgraph Event Producers
        csock@{ label: "Control socket", shape: event }
        fs@{ label: "Sync directory", shape: lin-cyl }
        ui@{ label: "Graphical user\ninterface", shape: person }
        start@{ label: "App startup", shape: console }
    end

    subgraph Publishers
        ctrl@{ label: "Control Listener", shape: subprocess }
        fwatch@{ label: "File Watcher", shape: subprocess }
        gui@{ label: "GUI Listener", shape: subprocess }
        sync@{ label: "Synchronizer", shape: subprocess }
    end

    subgraph Orchestrators
        broker@{ label: "Event Broker", shape: in-out }
        router@{ label: "Event Router", shape: out-in }
    end

    subgraph Subscribers
        cfg[Config\nUpdater]
        evt[Event\nAnnouncer]
        rcvr[File\nReceiver]
        sendr[File\nSender]
        tree[File-system\nTree Sender]
        fswrk[File-system\nworker]
    end

    subgraph Activities
        subgraph Local
            upcfg@{ label: "Update config file", shape: terminal}
            upfs@{ label: "Update sync directory", shape: terminal}
        end

        subgraph "Outward (to peers)"
            sevt@{ label: "Send event", shape: terminal }
            sdt@{ label: "Send data", shape: terminal }
        end
    end

    csock -. Peer request .-> ctrl --> broker
    fs -. File modification event .-> fwatch --> broker
    ui -. User request .-> gui --> broker
    start -. Initialization sync request .-> sync
    gui -. Forced sync request .-> sync --> broker

    broker -. Event envelope with metadata .-> router

    router --> evt -. Local sync events .-> sevt
    router --> sendr -. File requested by peer .-> sdt
    router --> tree -. Sync directory tree .-> sdt
    router --> cfg -. New settings .-> upcfg
    router --> rcvr -. File requested to peer .-> upfs
    router --> fswrk -. General FS work .-> upfs
```
