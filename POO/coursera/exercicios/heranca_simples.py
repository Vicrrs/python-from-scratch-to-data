# Herança simples -> usuários de uma API
class Usuario:
    def __init__(self, nome: str, email: str):
        self.nome = nome
        self.email = email

    def autenticar(self) -> str:
        return f"Usuários {self.email} autenticado."

class Administrador(Usuario):
    def excluir_usuario(self, email_usuario: str) -> str:
        return f"Usuário {email_usuario} excluído por {self.nome}."

admin = Administrador("Victor", "victor@empresa.com.br")
