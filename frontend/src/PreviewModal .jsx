import { Dialog } from "@headlessui/react";
import React from "react";

const PreviewModal = ({ isOpen, onClose, data, rowCount }) => {
  if (!isOpen) return null;

  return (
    <Dialog open={isOpen} onClose={onClose} className="fixed z-50 inset-0 overflow-y-auto ">
      <div className="flex items-center justify-center min-h-screen px-4">
        <Dialog.Panel className="bg-white p-6 rounded-xl shadow-xl max-w-4xl w-full overflow-x-auto">
          <Dialog.Title className="text-xl font-bold mb-4">Table Preview</Dialog.Title>
          <button onClick={onClose} className="absolute top-4 right-6 text-gray-600">Close</button>

          <table className="min-w-full text-sm text-left border">
            <thead className="bg-gray-100">
              <tr>
                {data[0]?.map((col, idx) => (
                  <th key={idx} className="px-3 py-2 border-b">{col}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.slice(1).map((row, i) => (
                <tr key={i}>
                  {row.map((cell, j) => (
                    <td key={j} className="px-3 py-2 border-b">{cell}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
          <p className="mt-2 text-gray-500">Rows: {rowCount}</p>
        </Dialog.Panel>
      </div>
    </Dialog>
  );
};

export default PreviewModal;
