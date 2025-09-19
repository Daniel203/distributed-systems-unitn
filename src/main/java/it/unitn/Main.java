package it.unitn;


public class Main {
    public static void main(String[] args) {
        Manager manager = new Manager();

    /*
        class Value {val int, version: int}
        key: Value 
        clientStorage

        nodes: Array<Node>
        nodesCrashed: Array<Node>

        Node:
        - id
        - storage (default)
        - get()

        Messaggi:
        Read(key)
        ReadResponse
        Write(key, value)
        WriteResponse
        Leave
        Join
        AskJoin (chiede tutti i nodi che ci sono nel sistema)
        Crash (main -> node)
        Recover
    */
    }

}
