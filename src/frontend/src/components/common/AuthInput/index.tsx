import { motion } from 'motion/react';
import { useState } from 'react';

import { descriptionVarients } from '@/styles/motions';

import * as S from './styles';

export type DescriptionType = 'normal' | 'valid' | 'error';
interface Description {
  type: DescriptionType;
  content: string;
}

interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  id: string;
  label: string;
  isRequired?: boolean;
  description?: Description;
  inputType?: 'text' | 'password' | 'email';
  value?: string;
  handleChange: (value: string) => void;
}

const AuthInput = ({
  id,
  label,
  isRequired = false,
  description,
  inputType = 'text',
  value,
  handleChange,
  ...props
}: InputProps) => {
  const [isFocused, setIsFocused] = useState(false);

  const handleFocus = () => setIsFocused(true);
  const handleBlur = () => setIsFocused(false);

  const isDescriptionDisplayed =
    description &&
    (description.type === 'error' || (isFocused && (description.type === 'normal' || description.type === 'valid')));

  return (
    <S.InputContainer id={id}>
      <S.InputLabel>
        {label} {isRequired && <S.RequiredMark>*</S.RequiredMark>}
      </S.InputLabel>
      <S.Input
        inputType={inputType}
        value={value}
        onFocus={handleFocus}
        onBlur={handleBlur}
        onChange={(event) => handleChange(event.target.value)}
        {...props}
      />
      <motion.div
        variants={descriptionVarients}
        initial="hidden"
        animate={isDescriptionDisplayed ? 'visible' : 'hidden'}
      >
        {isDescriptionDisplayed && (
          <S.DescriptionText $type={description.type}>{description.content}</S.DescriptionText>
        )}
      </motion.div>
    </S.InputContainer>
  );
};

export default AuthInput;
