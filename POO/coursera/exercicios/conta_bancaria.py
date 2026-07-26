# Métodos que alteram o estado do objeto

class ContaBancaria:
    def __init__(self, titular: str, saldo: float = 0):
        self.titular = titular
        self.saldo = saldo

    def depositar(self, valor: float) -> None:
        if valor <= 0:
            print("O depósito deve ser maior que zero.")
            return

        self.saldo += valor
        print(f"Depósito de R$ {valor:.2f} realizado.")

    def sacar(self, valor: float) -> None:
        if valor <= 0:
            print("O saque deve ser maior que zero.")

        if valor > self.saldo:
            print("Saldo insuficiente.")
            return

        self.saldo -= valor
        print(f"Saque de R$ {valor:.2f} realizado.")

    def consultar_saldo(self) -> str:
        return f"Saldo atual: R$ {self.saldo:.2f}"


conta = ContaBancaria("Victor Roza Souza", 16_000)

conta.depositar(500)
conta.sacar(300)

print(conta.consultar_saldo())
