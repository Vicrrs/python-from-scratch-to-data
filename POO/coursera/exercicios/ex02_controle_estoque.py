"""
Exercício 2: controle de estoque

Crie uma classe chamada Estoque com:

* nome do produto;
* quantidade disponível;
* método adicionar(quantidade);
* método remover(quantidade);
* método consultar().

Regras:

* não aceitar quantidade negativa;
* não permitir remover mais itens do que existem;
* mostrar uma mensagem quando a operação for realizada.
"""

class Estoque:
    def __init__(self, nome_produto: str, quantidade_disponivel: int):
        self.nome_produto = nome_produto
        self.quantidade_disponivel = quantidade_disponivel

    def adicionar(self, quantidade: int):
        if quantidade <= 0:
            print("A quantidade deve ser maior que zero.")
            return

        self.quantidade_disponivel += quantidade
        print(f"Adicionado {quantidade}")

    def remover(self, quantidade: int):
        if quantidade <= 0:
            print("A remoção deve ser maior que zero.")
            return

        if quantidade > self.quantidade_disponivel:
            print(f"Impossível remover essa quantidade, atualmente tem apenas {self.quantidade_disponivel} disponíveis.")
            return

        self.quantidade_disponivel -= quantidade
        print(f"Retirada de {quantidade}")

    def consultar(self) -> str:
        return f"Quantidade do estoque de {self.quantidade_disponivel}"



estoque = Estoque("Teclado", 10)

estoque.adicionar(0)
estoque.remover(3)

print(estoque.consultar())
