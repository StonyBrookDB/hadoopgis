
using namespace std;

bool intersects(Time_interval *a, Time_interval *b);
bool contains(Time_interval *a, Time_interval *b);
long mindist(Time_interval *a, Time_interval *b);


	bool intersects(Time_interval *a, Time_interval *b) {
		return (a->start <= b->end && b->start <= a->end);
	}

	bool contains(Time_interval *a, Time_interval *b) {
		return (a->start <= b->start && b->end <= a->end);
	}

	long mindist(Time_interval *a, Time_interval *b) {
		if (intersects(a, b)) {
			return 0;
		} else {
			return max(abs(a->start - b->end), abs(b->end - a->start));
		}
	}


TemporalObject::TemporalObject() {
	// Default constructor
}

TemporalObject::TemporalObject(string datestr, int supportedformat) {
	vector<string> fields;
	vector<string> innerfields;

	long tmpvalue;
	if (supportedformat == TEMPORAL_UNIX) {
		tokenize(datestr, fields, COMMA, true);
		for (size_t i = 0; i < fields.size(); i++) {
			tokenize(fields[i], innerfields, SPACE, true);
			if (innerfields.size() == 2) {
				elements.push_back(
						Time_interval(stol(innerfields[0], NULL, 10),
							stol(innerfields[1], NULL, 19)));
			} else {
				// size == 1
				elements.push_back(
						Time_interval(stol(innerfields[0], NULL, 10)));
				//time_interval(boost::lexical_cast<long>(fields[i])));
			}
			innerfields.clear();
		}

	} else if (supportedformat == TEMPORAL_LOCAL) {


	}	
	fields.clear();
}

TemporalObject::~TemporalObject() {
	elements.clear();
}

int TemporalObject::getNumIntervals() {
	return elements.size();
}

long TemporalObject::getStart() {
	return elements.front().start;
}

long TemporalObject::getEnd() {
	return elements.back().end;
}

bool TemporalObject::intersects(TemporalObject *obj) {
	for (size_t i = 0; i < elements.size(); i++) {
		for (size_t j = 0; j < obj->elements.size(); i++) {
			//if (intersects(&(elements[i]), &(obj->elements[j]))) {
			if (elements[i].start <= obj->elements[j].end
				&& elements[i].end >= obj->elements[j].start) {
				return true;
			}
		}
	}
	return false;	
}

bool TemporalObject::contains(TemporalObject *obj) {
	// Currently only implemented for single timestamp or single interval
	if (elements.size() == 1 && obj->elements.size() == 1) {
		//return contains(&(elements[0]), &(obj->elements[0]));
		return elements[0].start <= obj->elements[0].start 
			&& obj->elements[0].end <= elements[0].end;
	}
	return false;	
}

long TemporalObject::mindist(TemporalObject *obj) {
	// Currently only implemented for single timestamp or single interval
	if (elements.size() == 1 && obj->elements.size() == 1) {
		if (elements[0].start <= obj->elements[0].end
			&& elements[0].end >= obj->elements[0].start) {
			return 0;
		} else {
			return max(abs(elements[0].start - obj->elements[0].end), 
					abs(elements[0].end - obj->elements[0].start));
		}
	}
	return false;	
}

