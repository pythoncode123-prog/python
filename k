import React, { useState, useRef, useEffect } from 'react';

function SearchableCombobox({ options, value, onChange, placeholder = "Search or select..." }) {
  const [searchTerm, setSearchTerm] = useState(value || '');
  const [isOpen, setIsOpen] = useState(false);
  const dropdownRef = useRef(null);
  
  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
        setIsOpen(false);
      }
    };
    
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);
  
  // Update search term when value changes
  useEffect(() => {
    setSearchTerm(value || '');
  }, [value]);
  
  const handleInputChange = (e) => {
    setSearchTerm(e.target.value);
    setIsOpen(true);
  };
  
  const handleSelect = (option) => {
    setSearchTerm(option);
    setIsOpen(false);
    onChange({ target: { value: option } });
  };
  
  // Filter options based on search term
  const filteredOptions = options.filter(option => 
    option.toLowerCase().includes(searchTerm.toLowerCase())
  );
  
  return (
    <div ref={dropdownRef} style={{ position: 'relative' }}>
      <input
        type="text"
        className="w-full p-1 border rounded"
        value={searchTerm}
        onChange={handleInputChange}
        onFocus={() => setIsOpen(true)}
        placeholder={placeholder}
      />
      
      {isOpen && (
        <div 
          className="absolute z-10 w-full mt-1 bg-white border rounded shadow-lg max-h-60 overflow-auto"
          style={{ top: '100%' }}
        >
          <div
            className="px-3 py-2 hover:bg-gray-100 cursor-pointer font-semibold"
            onClick={() => handleSelect('')}
          >
            All
          </div>
          {filteredOptions.length > 0 ? (
            filteredOptions.map((option) => (
              <div
                key={option}
                className="px-3 py-2 hover:bg-gray-100 cursor-pointer"
                onClick={() => handleSelect(option)}
              >
                {option}
              </div>
            ))
          ) : (
            <div className="px-3 py-2 text-gray-500 italic">No options found</div>
          )}
        </div>
      )}
    </div>
  );
}

// Modified version of your createDropdownCell function
function createDropdownCell() {
  const handleChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    setValue(e.target.value);
    console.log("Selected:", e.target.value);
  };
  
  let colIds = ['master_cr',
    'servername',
    'serialno',
    'validation_cr',
    'hall',
    'location',
    'os',
    'class',
    'master_cr_state',
    'status',
    'hardware',
    'env',
    'building',
    'deviceid',
    'cs_order_id',
    'cs_task_id'];
    
  let columns: any = [];
  for (let colId of colIds) {
    let options: any = getOptionsForColumn(colId);
    console.log(options)
    columns[colId] = (
      <SearchableCombobox
        options={options.map((option) => option.value || option)}
        value={value || ''}
        onChange={handleChange}
        placeholder={`Search ${colId}...`}
      />
    );
  }
  
  return columns;
}
