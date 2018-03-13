Create inverted index for 20NewsGroups dataset

Dataset : http://qwone.com/~jason/20Newsgroups/20news-bydate.tar.gz

Download and remove the stop words. Inverted Index is calculated by the following formula

![alt text](https://github.com/RadhikaKalaiselvan/Big-data/blob/master/Inverted%20Index/formula.png)

For every word in each document the map phase produces ((word, document name),1)

For every distinct word in the document ((word,'*'),1) is generated.

In map phase the sum of all values of key (word,*) will give you ni(Number of documents with the word)

Now for every document key (word,document name) the sum of values will given tf (term frequency - no. of times the word occurs in this document)

Using these two values the weight is assigned to each ((word,weight),document Name), using this output we can list the top 10 document which is most relavant to the search word.

Google search using similar implementation to list web pages for you search words.
