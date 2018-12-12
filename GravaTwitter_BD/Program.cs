using System;
using System.Collections.Generic;
using Tweetinvi;
using System.Data.Odbc;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Net;

namespace GravaTwitter_BD
{
    class keywords
    {
        public string key { get; set; }
    }

    class Program
    {

        //RECUPERA AS KEYWORDS ARMAZENADAS NO BANCO 
        public List<keywords> getKeyWords()
        {

            string tx_key = "";
            //CONEXAO DO BANCO
            OdbcConnection conexao = new OdbcConnection(getBDCredentials());
            //COMMANDS BANCODADOS
            OdbcCommand comando = conexao.CreateCommand();
            OdbcCommand comandoKeys = conexao.CreateCommand();
            //LEITORES DO BANCO 
            OdbcDataReader canais;
            OdbcDataReader dataKey;


            //LISTA DE PALAVRAS CHAVES
            List<keywords> listKeys = new List<keywords>();
            conexao.Open();

            comando.CommandText = "SELECT id_canal FROM iptv2_canal WHERE lg_excluido = 'N' ORDER BY id_canal";
            canais = comando.ExecuteReader();

            if(canais.HasRows)
            {
                while( canais.Read())
                {
                    comandoKeys.CommandText = "SELECT nm_hashtag,id_hashtag FROM iptv2_hashtag WHERE id_canal = " + canais.GetValue(0) + " ORDER BY nm_hashtag";
                    dataKey = comandoKeys.ExecuteReader();
                    if(dataKey.HasRows)
                    {
                        tx_key = "";

                        while (dataKey.Read())
                        { 
                            listKeys.Add(new keywords() { key = dataKey.GetValue(0).ToString() });
                        }
                    }
                    dataKey.Close();
                }
            }
            conexao.Close();
            return listKeys;
        }


        //RECUPERA AS CREDENCIAIS DO BANCO DE DADOS
        public string getBDCredentials()
        {
            System.IO.StreamReader file = new System.IO.StreamReader("credenciais/conexao_bd.txt");
            string tx_conexao = file.ReadLine();
            file.Close();
            return tx_conexao;
        }

        //FUNCAO PARA INSERIR OS TWEETS NO BANDO DE DADOS
        public void insertTweet(string nm_imagem, string nm_usuario, DateTime dt_tweet, string tx_tweet, string id_hashtag)
        {
            string queryInsert = "INSERT INTO IPTV2_TWEET(ID_TWEET, DT_TWEET, TX_TWEET, ID_HASHTAG, NM_IMAGEM, NM_USUARIO) VALUES(SEQ_IPTV2_TWEET.nextval, TO_DATE('" + dt_tweet + "', 'DD/MM/YYYY HH24:MI:SS'),'" + tx_tweet + "'," + id_hashtag + ",'" + nm_imagem + "','" + nm_usuario + "')";

            OdbcConnection con = new OdbcConnection(getBDCredentials());
            OdbcCommand ins = con.CreateCommand();
            con.Open();

            ins.CommandText = queryInsert;
            try
            {
                ins.ExecuteNonQuery();
            }catch(System.Data.Odbc.OdbcException)
            {
                Console.WriteLine("Não foi possível adicionar Tweet no banco");
            }
           

            con.Close();
            
        }

        //FUNCAO PARA ACHAR O ID DA PALAVRA CHAVE DO TWITTER NO BANCO DE DADOS
        public string getIDkeyword(string keyword)
        {

            OdbcConnection conexao = new OdbcConnection(getBDCredentials());
            OdbcCommand comando;
            OdbcDataReader dataReader;
            comando = conexao.CreateCommand();
            string resposta = "";
            string query = "SELECT ID_HASHTAG FROM IPTV2_HASHTAG WHERE NM_HASHTAG LIKE '" + keyword + "'";


            comando.CommandText = query;
            conexao.Open();

            dataReader = comando.ExecuteReader();
            if(dataReader.HasRows)
            {
                while(dataReader.Read())
                {
                    resposta = dataReader.GetValue(0).ToString();
                }
            }

            return resposta;
            
        }



        //FAZ A CONEXÃO COM A API DO TWITTER
        public void getAPIConnectionTwiiter()
        {
            //LISTA DE KEYWORDS
            List<keywords> listKeys = getKeyWords();

            //AUTENTICAÇÃO COM A API
            Auth.SetUserCredentials();

            //CRIANDO O OBJETO STREAM   
            var stream = Stream.CreateFilteredStream();

            //ADICIONANDO A KEYWORD DE BUSCA DA LISTA
            foreach(keywords keys in listKeys)
            {
                stream.AddTrack(keys.key);
            }

            Console.WriteLine("Iniciando a conexão com a API do Twitter");

            stream.MatchingTweetReceived += (sender, arguments) =>
            {
                Console.WriteLine("Tweet : "+ WebUtility.HtmlEncode(arguments.Tweet.ToString()));
                Console.WriteLine("Criado em : " + arguments.Tweet.CreatedAt);
                Console.WriteLine("Nome de usuário: " + WebUtility.HtmlEncode(arguments.Tweet.CreatedBy.ScreenName));
                Console.WriteLine("imagem : " + arguments.Tweet.CreatedBy.ProfileImageUrl);

                try
                {                  
                    insertTweet(arguments.Tweet.CreatedBy.ProfileImageUrl, WebUtility.HtmlEncode(arguments.Tweet.CreatedBy.ScreenName), arguments.Tweet.CreatedAt, WebUtility.HtmlEncode(arguments.Tweet.ToString()), getIDkeyword(arguments.MatchingTracks.GetValue(0).ToString()));                    
                }
                catch (System.IndexOutOfRangeException)
                {

                }
            };
        
            stream.StartStreamMatchingAllConditions();
        }

        static void Main(string[] args)
        {
            Program obj = new Program();

            obj.getAPIConnectionTwiiter();
            
        }
    }
}
