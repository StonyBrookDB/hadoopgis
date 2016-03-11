#include <string>
#include <vector>

using namespace std;

void tokenize ( const string& str, vector<string>& result,
	const string& delimiters = " ,;:\t", 
	const bool keepBlankFields=false,
	const string& quote="\"\'"
	)
{
    // clear the vector
    if ( false == result.empty() )
    {
	result.clear();
    }

    // you must be kidding
    if (delimiters.empty())
	return ;

    string::size_type pos = 0; // the current position (char) in the string
    char ch = 0; // buffer for the current character
    char delimiter = 0;	// the buffer for the delimiter char which
    // will be added to the tokens if the delimiter
    // is preserved
    char current_quote = 0; // the char of the current open quote
    bool quoted = false; // indicator if there is an open quote
    string token;  // string buffer for the token
    bool token_complete = false; // indicates if the current token is
    // read to be added to the result vector
    string::size_type len = str.length();  // length of the input-string

    // for every char in the input-string
    while ( len > pos )
    {
	// get the character of the string and reset the delimiter buffer
	ch = str.at(pos);
	delimiter = 0;

	bool add_char = true;

	// check ...

	// ... if the delimiter is a quote
	if ( false == quote.empty())
	{
	    // if quote chars are provided and the char isn't protected
	    if ( string::npos != quote.find_first_of(ch) )
	    {
		// if not quoted, set state to open quote and set
		// the quote character
		if ( false == quoted )
		{
		    quoted = true;
		    current_quote = ch;

		    // don't add the quote-char to the token
		    add_char = false;
		}
		else // if quote is open already
		{
		    // check if it is the matching character to close it
		    if ( current_quote == ch )
		    {
			// close quote and reset the quote character
			quoted = false;
			current_quote = 0;

			// don't add the quote-char to the token
			add_char = false;
		    }
		} // else
	    }
	}

	if ( false == delimiters.empty() && false == quoted )
	{
	    // if ch is delemiter 
	    if ( string::npos != delimiters.find_first_of(ch) )
	    {
		token_complete = true;
		// don't add the delimiter to the token
		add_char = false;
	    }
	}

	// add the character to the token
	if ( true == add_char )
	{
	    // add the current char
	    token.push_back( ch );
	}

	// add the token if it is complete
	// if ( true == token_complete && false == token.empty() )
	if ( true == token_complete )
	{
	    if (token.empty())
	    {
		if (keepBlankFields)
		    result.push_back("");
	    }
	    else 
		// add the token string
		result.push_back( token );

	    // clear the contents
	    token.clear();

	    // build the next token
	    token_complete = false;

	}
	// repeat for the next character
	++pos;
    } // while
    
    /* 
    cout << "ch: " << (int) ch << endl;
    cout << "token_complete: " << token_complete << endl;
    cout << "token: " << token<< endl;
    */
    // add the final token
    if ( false == token.empty() ) {
	result.push_back( token );
    }
    else if(keepBlankFields && string::npos != delimiters.find_first_of(ch) ){
	result.push_back("");
    }
}

