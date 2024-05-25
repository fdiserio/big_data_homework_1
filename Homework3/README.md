# big_data_homework_1

Il seguente progetto riporta un'analisi dettagliata di due dataset cinematrografici, su cui sono state effettuate le seguenti query:
1. Film Comuni
2. Tutti i Film
3. TOP 10
4. FLOP 10
5. Film più votato
6. Film più recensito
7. Film per genere
8. Media Voti per genere
9. Durata Media di un Film
10. Durata Media di un Film per genere
11. Rating Ponderato
12. Top film per cadenza decennale
13. Top Decennio
14. Mean Decennio
15. Attori in più film
16. Attori più presenti nei film migliori
17. Attori e Registri con più collaborazioni
18. Parole più ricorrenti nei commenti della Top 10
19. Parole più ricorrenti nei commenti della Flop 10

Per l'esecuzione del progetto bisogna effettuare i seguenti passi:
1. Installare un ambiente virtuale 'venv' all'interno della cartella di progetto:
    - python -m venv venv
    - venv/Scripts/Activate.ps1
2. Da terminale (in ambiente virtuale), lanciare il seguente comando per l'installazione delle librerie:
    **pip install -r .\requirements.txt**
3. Modificare la stringa "uri" all'interno dei file "LoadData.py" e "Homepage.py" per la connessione al proprio cluster su MongoDB
4. Da terminale (in ambiente virtuale), lanciare il seguente comando per creare le collection sul cluster:
    **python .\LoadData.py**
5. Da terminale (in ambiente virtuale), lanciare il seguente comando per avviare la dashboard streamlit:
    **streamlit run .\Homepage.py**

N.B. Il progetto è stato svolto utilizzando la versione di Python 3.11.
