"""
Exercício 4: nota protegida

Crie uma classe chamada Aluno com:

nome;
atributo privado __nota;
propriedade nota;
setter para validar a nota;
método verificar_situacao().

A nota deve estar entre 0 e 10.

A situação deve seguir esta regra:

Nota menor que 5: Reprovado
Nota entre 5 e 6.9: Recuperação
Nota igual ou maior que 7: Aprovado

Exemplo:

aluno = Aluno("Maria", 8.5)

print(aluno.nota)
print(aluno.verificar_situacao())
"""

class Aluno:
    def __init__(self, nome: str, nota: float):
        self.nome = nome
        self.__nota = 0
        self.nota = nota

    @property
    def nota(self) -> float:
        return self.__nota

    @nota.setter
    def nota(self, valor):
        """Setter que valida se a nota esta entre 0 e 10"""
        if 0<= valor <= 10:
            self.__nota = valor
        else:
            raise ValueError("A nota deve estar entre 0 e 10.")

    def verificar_situacao(self):
        """Retorna a situação do aluno com base na nota."""
        if self.__nota < 5:
            return "Reprovado"
        elif self.__nota < 7:
            return "Recuperacao"
        else:
            return "Aprovado"

# Exemplo de uso
if __name__ == "__main__":
    aluno = Aluno("Maria", 8.5)
    print(aluno.nota)                  # 8.5
    print(aluno.verificar_situacao())  # Aprovado

    # Teste com outras notas
    aluno2 = Aluno("João", 4.9)
    print(aluno2.nota)                 # 4.9
    print(aluno2.verificar_situacao()) # Reprovado

    aluno3 = Aluno("Ana", 6.5)
    print(aluno3.nota)                 # 6.5
    print(aluno3.verificar_situacao()) # Recuperação

